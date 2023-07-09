package chunkserver

import (
	"main/src/gfs"
	"main/src/gfs/util"
	log "main/src/github.com/Sirupsen/logrus"
	"net"
	"net/rpc"
	"os"
	"sync"
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
	sync.RWMutex
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
	cs.dl.Set(dataID, data)
	var errorCode gfs.ErrorCode
	for _, address := range to {
		if address != cs.address {
			forwardArg := gfs.ForwardDataArg{
				DataID: dataID,
				Data:   data,
			}
			forwardReply := new(gfs.ForwardDataReply)
			util.Call(address, "ChunkServer.PRCForwardData", forwardArg, forwardReply)
			if forwardReply.ErrorCode != 0 {
				errorCode = forwardReply.ErrorCode
			}
		}
	}
	reply.ErrorCode = errorCode
	reply.DataID = dataID
	return nil
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	cs.dl.Set(args.DataID, args.Data)
	reply.ErrorCode = 0
	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	_, ok := cs.chunk[args.Handle]
	if ok {
		reply.ErrorCode = 1
		return nil
	}
	cs.chunk[args.Handle] = &chunkInfo{
		RWMutex:       sync.RWMutex{},
		length:        0,
		version:       0,
		newestVersion: 0,
		mutations:     make(map[gfs.ChunkVersion]*Mutation),
	}
	os.Chdir(cs.serverRoot)
	os.Create(string(args.Handle))
	reply.ErrorCode = 0
	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {

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
	if !ok {
		return gfs.Error{
			Code: 1,
			Err:  "CantFindDataWhenAppend",
		}
	}
	chunk.Lock()
	defer chunk.Unlock()
	chunk.newestVersion += 1
	if len(data)+int(offset) > int(chunk.length) {
		chunk.length = gfs.Offset(len(data) + int(offset))
	}
	chunk.mutations[chunk.newestVersion] = &Mutation{
		mtype:   gfs.MutationWrite,
		version: chunk.newestVersion,
		data:    data,
		offset:  offset,
	}
	os.Chdir(cs.serverRoot)
	file, _ := os.Open(string(args.Handle))
	file.WriteAt(data, int64(args.Offset))
	file.Close()
	flag := true
	for _, secondary := range secondaries {
		writeArgs := gfs.WriteChunkArg{
			Handle:      handle,
			DataID:      dataID,
			Offset:      offset,
			Secondaries: make([]gfs.ServerAddress, 0),
		}
		writeReply := new(gfs.WriteChunkReply)
		util.Call(secondary, "ChunkServer.RPCWriteChunk", writeArgs, writeReply)
		flag = flag && (writeReply.ErrorCode == 0)
	}
	if flag {
		reply.ErrorCode = 0
	} else {
		reply.ErrorCode = 1
	}
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
	chunk := cs.chunk[args.Handle]
	chunk.Lock()
	defer chunk.Unlock()
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
	chunk.length = gfs.Offset(len(data)) + chunk.length
	flag := true
	for _, secondary := range secondaries {
		writeArgs := gfs.AppendChunkArg{
			Handle:      handle,
			DataID:      dataID,
			Secondaries: make([]gfs.ServerAddress, 0),
		}
		writeReply := new(gfs.AppendChunkReply)
		util.Call(secondary, "ChunkServer.RPCAppendChunk", writeArgs, writeReply)
		flag = flag && (writeReply.ErrorCode == 0)
	}
	if flag {
		reply.ErrorCode = 0
	} else {
		reply.ErrorCode = 1
	}
	return nil
}

// RPCApplyMutation is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {

}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	chunk := cs.chunk[args.Handle]
	chunk.RLock()
	defer chunk.RUnlock()
	os.Chdir(cs.serverRoot)
	file, _ := os.Open(string(args.Handle))
	data := make([]byte, gfs.MaxChunkSize)
	file.Read(data)
	file.Close()
	applyArgs := gfs.ApplyCopyArg{
		Handle:  0,
		Data:    data,
		Version: chunk.version,
	}
	applyReply := new(gfs.ApplyCopyReply)
	err := util.Call(args.Address, "ChunkServer.RPCApplyCopy", applyArgs, applyReply)
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
	chunk := cs.chunk[args.Handle]
	chunk.Lock()
	defer chunk.Unlock()
	chunk.version = args.Version
	os.Chdir(cs.serverRoot)
	file, _ := os.Open(string(args.Handle))
	file.WriteAt(args.Data, 0)
	file.Close()
	reply.ErrorCode = 0
	return nil
}
