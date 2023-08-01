package chunkserver

import (
	"gfsmain/src/gfs"
	"gfsmain/src/gfs/util"
	log "gfsmain/src/github.com/Sirupsen/logrus"
	"github.com/sasha-s/go-deadlock"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"time"
)

// ChunkServer struct
type ChunkServer struct {
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to true if server is shutdown

	dl                     *downloadBuffer                // expiring download buffer
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	chunk                  map[gfs.ChunkHandle]*chunkInfo // chunk information
}

type Mutation struct {
	mtype   gfs.MutationType
	version gfs.ChunkVersion
	data    []byte
	offset  gfs.Offset
}

type chunkInfo struct {
	deadlock.RWMutex
	length        gfs.Offset
	version       gfs.ChunkVersion               // version number of the chunk in disk
	newestVersion gfs.ChunkVersion               // allocated newest version number
	mutations     map[gfs.ChunkVersion]*Mutation // mutation buffer
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:                addr,
		shutdown:               make(chan struct{}),
		master:                 masterAddr,
		serverRoot:             serverRoot,
		dl:                     newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk:                  make(map[gfs.ChunkHandle]*chunkInfo),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// if chunk server is dead, ignores connection error
				if !cs.dead {
					log.Fatal(err)
				}
			}
		}
	}()

	// Heartbeat
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			pe := cs.pendingLeaseExtensions.GetAllAndClear()
			le := make([]gfs.ChunkHandle, len(pe))
			for i, v := range pe {
				le[i] = v.(gfs.ChunkHandle)
			}
			args := &gfs.HeartbeatArg{
				Address:         addr,
				LeaseExtensions: le,
			}
			if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
				log.Fatal("heartbeat rpc error ", err)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

//func (cs *ChunkServer) persist() {
//	w := new(bytes.Buffer)
//	e:=gob.NewEncoder(w)
//	for handle, info := range cs.chunk {
//		info.RLock()
//		e.Encode(handle)
//		e.Encode(info)
//		info.RUnlock()
//	}
//}
//
//func (cs *ChunkServer) readPersist{
//
//}
//
//func (cs *ChunkServer) applyMutation{
//	for handle, info := range cs.chunk {
//		info.Lock()
//		if info.mutations
//	}
//}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	to, data, handle := args.ForwardTo, args.Data, args.Handle
	dataID := cs.dl.New(handle)
	util.ServerLogf(cs.address, "assign DataID:%v", dataID.TimeStamp)
	cs.dl.Set(dataID, data)
	if len(to) > 0 {
		ch := make(chan gfs.ErrorCode, len(to)-1)
		for _, address := range to {
			address := address
			go func(serverAddress gfs.ServerAddress) {
				if address != cs.address {
					{
						forwardArg := gfs.ForwardDataArg{
							DataID: dataID,
							Data:   data,
						}
						forwardReply := new(gfs.ForwardDataReply)
						util.ServerLogf(cs.address, "Send PushDataRPC to%s ID:%v", address, dataID.TimeStamp)
						util.Call(address, "ChunkServer.RPCForwardData", forwardArg, forwardReply)
						ch <- reply.ErrorCode
					}
				}
			}(address)
		}
		cnt := 0
		for code := range ch {
			//util.ServerLogf(cs.address, "channel handle PushDataRPC")
			if code != 0 {
				reply.ErrorCode = code
			}
			cnt += 1
			if cnt == cap(ch) {
				break
			}
		}
	}
	reply.DataID = dataID
	return nil
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	util.ServerLogf(cs.address, "Receive DataID %v", args.DataID.TimeStamp)
	cs.dl.Set(args.DataID, args.Data)
	reply.ErrorCode = 0
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	util.ServerLogf(cs.address, "Create chunk %s", args.Handle)
	_, ok := cs.chunk[args.Handle]
	if ok {
		reply.ErrorCode = 1
		return nil
	}
	cs.chunk[args.Handle] = &chunkInfo{
		RWMutex:       deadlock.RWMutex{},
		length:        0,
		version:       0,
		newestVersion: 0,
		mutations:     make(map[gfs.ChunkVersion]*Mutation),
	}
	_, err := os.Create(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)))
	if err != nil {
		log.Fatal("chunk server cannot create chunk", err)
	}
	reply.ErrorCode = 0
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	file, err := os.OpenFile(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)), os.O_RDWR, 0755)
	if err != nil {
		log.Fatal("chunk server cannot open file", err)
	}
	//siz := int(cs.chunk[args.Handle].length)
	//if siz < args.Length {
	//	siz = args.Length
	//}
	data := make([]byte, args.Length)
	_, err = file.ReadAt(data, int64(args.Offset))
	util.ServerLogf(cs.address, "read %v bytes of chunk %v at offset %v", args.Length, args.Handle, args.Offset)
	file.Close()
	if err != nil {
		return err
	}
	reply.Length = args.Length
	reply.Data = data
	reply.ErrorCode = 0
	return nil
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	offset := args.Offset
	secondaries := args.Secondaries
	dataID := args.DataID
	handle := args.Handle
	chunk := cs.chunk[handle]
	data, ok := cs.dl.Get(dataID)
	util.ServerLogf(cs.address, "write bytes of length %v at offset %v ID:%v", len(data), offset, dataID.TimeStamp)
	if !ok {
		return gfs.Error{
			Code: 1,
			Err:  "CantFindDataWhenAppend",
		}
	}
	chunk.Lock()
	defer chunk.Unlock()
	chunk.newestVersion += 1
	chunk.mutations[chunk.newestVersion] = &Mutation{
		mtype:   gfs.MutationWrite,
		version: chunk.newestVersion,
		data:    data,
		offset:  offset,
	}
	file, err := os.OpenFile(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)), os.O_RDWR, 0755)
	if err != nil {
		log.Fatal("chunk server cannot open file ", err)
	}
	if offset > chunk.length {
		pad := make([]byte, offset-chunk.length)
		file.WriteAt(pad, int64(chunk.length))
	}
	_, err = file.WriteAt(data, int64(args.Offset))
	if err != nil {
		log.Fatal("chunk server cannot write file", err)
	}
	file.Close()
	if len(data)+int(offset) > int(chunk.length) {
		chunk.length = gfs.Offset(len(data) + int(offset))
	}
	//util.ServerLogf(cs.address, "Done write for chunk %s ID:%v", args.Handle, args.DataID.TimeStamp)
	if len(secondaries) != 0 {
		ch := make(chan gfs.ErrorCode, len(secondaries))
		for _, secondary := range secondaries {
			secondary := secondary
			go func(gfs.ServerAddress) {
				writeArgs := gfs.WriteChunkArg{
					Handle:      handle,
					DataID:      dataID,
					Offset:      offset,
					Secondaries: make([]gfs.ServerAddress, 0),
				}
				writeReply := new(gfs.WriteChunkReply)
				//todo uncertain error handle
				util.ServerLogf(cs.address, "repost write to %v", secondary)
				util.Call(secondary, "ChunkServer.RPCWriteChunk", writeArgs, writeReply)
				ch <- writeReply.ErrorCode
			}(secondary)
		}
		cnt := 0
		for err2 := range ch {
			if err2 != 0 {
				reply.ErrorCode = err2
			}
			cnt += 1
			if cnt == cap(ch) {
				break
			}
		}
	}
	//println("damn")
	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	dataId := args.DataID
	secondaries := args.Secondaries
	dataID := args.DataID
	handle := args.Handle
	data, ok := cs.dl.Get(dataId)
	if !ok {
		return gfs.Error{
			Code: 1,
			Err:  "CantFindDataWhenAppend",
		}
	}
	util.ServerLogf(cs.address, "append bytes of len %v ID:%v", len(data), dataID.TimeStamp)
	chunk := cs.chunk[args.Handle]
	chunk.Lock()
	if int(chunk.length)+len(data) > gfs.MaxChunkSize {
		chunk.newestVersion += 1
		chunk.mutations[chunk.newestVersion] = &Mutation{
			mtype:   gfs.MutationPad,
			version: chunk.newestVersion,
			data:    nil,
			offset:  0,
		}
		chunk.length = gfs.MaxChunkSize
		reply.ErrorCode = gfs.AppendExceedChunkSize
		reply.Offset = 0
		return nil
	}
	reply.Offset = cs.chunk[handle].length
	chunk.newestVersion += 1
	chunk.mutations[chunk.newestVersion] = &Mutation{
		mtype:   gfs.MutationAppend,
		version: chunk.newestVersion,
		data:    data,
		offset:  reply.Offset,
	}
	file, err := os.OpenFile(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)), os.O_RDWR, 0755)
	if err != nil {
		log.Fatal("chunk server cannot open file ", err)
	}
	_, err = file.WriteAt(data, int64(chunk.length))
	if err != nil {
		log.Fatal("chunk server cannot append file", err)
	}
	file.Close()
	chunk.length = gfs.Offset(len(data)) + chunk.length
	chunk.Unlock()
	if len(secondaries) != 0 {
		ch := make(chan gfs.ErrorCode, len(secondaries))
		for _, secondary := range secondaries {
			if secondary == cs.address {
				log.Panic("identical")
			}
			appendArgs := gfs.AppendChunkArg{
				Handle:      handle,
				DataID:      dataID,
				Secondaries: make([]gfs.ServerAddress, 0),
			}
			appendReply := new(gfs.AppendChunkReply)
			util.ServerLogf(cs.address, "repost append to %v", secondary)
			err := util.Call(secondary, "ChunkServer.RPCAppendChunk", appendArgs, appendReply)
			if err != nil {
				return err
			}
			ch <- reply.ErrorCode
		}
		cnt := 0
		for err := range ch {
			if err != 0 {
				reply.ErrorCode = err
			}
			cnt += 1
			if cnt == cap(ch) {
				break
			}
		}
	}
	return nil
}

//// RPCApplyMutation is called by primary to apply mutations
//func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
//
//}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	chunk := cs.chunk[args.Handle]
	chunk.RLock()
	defer chunk.RUnlock()
	file, err := os.OpenFile(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)), os.O_RDWR, 0755)
	if err != nil {
		log.Fatal("chunk server cannot open file", err)
	}
	data := make([]byte, chunk.length)
	_, err = file.ReadAt(data, 0)
	if err != nil {
		return err
	}
	file.Close()
	applyArgs := gfs.ApplyCopyArg{
		Handle:  args.Handle,
		Data:    data,
		Version: chunk.version,
	}
	applyReply := new(gfs.ApplyCopyReply)
	util.ServerLogf(cs.address, "forward chunk %v to server %v", args.Handle, args.Address)
	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy", applyArgs, applyReply)
	if err != nil {
		reply.ErrorCode = 1
	} else {
		reply.ErrorCode = 0
	}
	return nil
}

// RPCApplyCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	cs.chunk[args.Handle] = &chunkInfo{
		length:        0,
		version:       0,
		newestVersion: 0,
		mutations:     make(map[gfs.ChunkVersion]*Mutation),
	}
	util.ServerLogf(cs.address, "apply replica of chunk %v", args.Handle)
	chunk := cs.chunk[args.Handle]
	chunk.Lock()
	defer chunk.Unlock()
	chunk.version = args.Version
	chunk.newestVersion = args.Version
	chunk.length = gfs.Offset(len(args.Data))
	os.Remove(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)))
	_, err := os.Create(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)))
	if err != nil {
		log.Fatal("chunk server cannot create file ", err)
	}
	file, err := os.OpenFile(path.Join(cs.serverRoot, strconv.FormatInt(int64(args.Handle), 10)), os.O_RDWR, 0755)
	if err != nil {
		log.Fatal("chunk server cannot open file ", err)
	}
	_, err = file.WriteAt(args.Data, 0)
	if err != nil {
		log.Fatal("chunk server cannot write file ", err)
	}
	file.Close()
	reply.ErrorCode = 0
	return nil
}
