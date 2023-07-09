package master

import (
	"main/src/gfs/util"
	log "main/src/github.com/Sirupsen/logrus"
	"net"
	"net/rpc"
	"time"

	"main/src/gfs"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string            // path to metadata storage
	l          net.Listener
	shutdown   chan struct{}

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
				log.Exit(1)
			}
		}
	}()

	// Background Task
	go func() {
		ticker := time.Tick(gfs.BackgroundInterval)
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			<-ticker

			err := m.BackgroundActivity()
			if err != nil {
				log.Fatal("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	m.csm.Heartbeat(args.Address)
	for _, handle := range args.LeaseExtensions {
		err := m.cm.ExtendLease(handle, args.Address)
		if err != nil {
			return err
		}
	}
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	curLease, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}
	reply.Primary = curLease.primary
	reply.Secondaries = curLease.secondaries
	reply.Expire = curLease.expire
	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	replicas, err := m.cm.GetReplicas(args.Handle)
	if err != nil {
		return err
	}
	reply.Locations = replicas.CastAllToServerAddress()
	return nil
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	err := m.nm.Create(args.Path)
	if err != nil {
		return err
	}
	servers, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
	if err != nil {
		return err
	}
	handle, err := m.cm.CreateChunk(args.Path, servers)
	if err != nil {
		return err
	}
	for _, server := range servers {
		go func(server gfs.ServerAddress, handle gfs.ChunkHandle) {
			cnt := 0
			for {
				cnt += 1
				createArgs := gfs.CreateChunkArg{Handle: handle}
				createReply := new(gfs.CreateChunkReply)
				util.Call(server, "ChunkServer.CreateChunk", createArgs, createReply)
				if createReply.ErrorCode == 0 {
					break
				}
			}
			if cnt > 10 {
				log.Fatal("Unexpected failure in chunk ", handle, "create for ", server)
			}
		}(server, handle)

	}
	return nil
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, replay *gfs.MkdirReply) error {
	err := m.nm.Mkdir(args.Path)
	if err != nil {
		return err
	}
	return nil
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	pathInfo, err := m.nm.QueryFile(args.Path)
	if err != nil {
		return err
	}
	reply.IsDir = pathInfo.IsDir
	reply.Length = pathInfo.Length
	reply.Chunks = pathInfo.Chunks
	return nil
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	chunk, err := m.cm.GetChunk(args.Path, args.Index)
	if err != nil {
		return err
	}
	reply.Handle = chunk
	return nil
}

func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	files, err := m.nm.QueryDir(args.Path)
	reply.Files = files
	if err != nil {
		return err
	}
	return nil
}
