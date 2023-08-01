package master

import (
	"gfsmain/src/gfs/util"
	log "gfsmain/src/github.com/Sirupsen/logrus"
	"net"
	"net/rpc"
	"time"

	"gfsmain/src/gfs"
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
	go m.DeadChunkServerHandling()
	go m.RefillReplica()
	return nil
}

func (m *Master) DeadChunkServerHandling() {
	deadServers := m.csm.DetectDeadServers()
	for _, server := range deadServers {
		util.MasterLogf("chunkserver %v is detected to be dead", server)
		handles, err := m.csm.RemoveServer(server)
		if err != nil {
			log.Fatal(err)
		}
		for _, handle := range handles {
			m.cm.RemoveReplica(handle, server)
		}
	}
}

func (m *Master) RefillReplica() {
	handles := m.cm.DetectInadequateReplica()
	for _, handle := range handles {
		handle := handle
		go func() {
			replicas, _ := m.cm.GetReplicas(handle)
			addr := m.csm.ReallocateChunk(handle, replicas.CastAllToServerAddress())
			err := m.cm.ReallocateReplica(handle, addr)
			if err != nil {
				log.Warningf("error occur in reallocating replica %v, %v", handle, err)
				m.cm.AddHistory(handle)
			}
		}()
	}
}

func (m *Master) Backup() {

}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	//util.MasterLogf("Receive Heartbeat from ChunkServer:%s", args.Address)
	m.csm.Heartbeat(args.Address)
	go m.cm.ExtendLease(args.LeaseExtensions, args.Address)
	//util.MasterLogf("heartbeat of %v", args.Address)
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
	util.MasterLogf("Create file {%s}", args.Path)
	err := m.nm.Create(args.Path)
	if err != nil {
		return err
	}
	err, _ = m.createChunk(args.Path)
	return err
}

func (m *Master) createChunk(path gfs.Path) (error, gfs.ChunkHandle) {
	servers := m.csm.ChooseServers(gfs.DefaultNumReplicas)
	handle := gfs.NewHandle()
	m.cm.RegisterChunk(path, handle)
	util.MasterLogf("Send CreateChunkPRC to %s, %s, %s", servers[0], servers[1], servers[2])
	for _, server := range servers {
		go func(server gfs.ServerAddress, handle gfs.ChunkHandle) {
			createArgs := gfs.CreateChunkArg{Handle: handle}
			createReply := new(gfs.CreateChunkReply)
			util.Call(server, "ChunkServer.RPCCreateChunk", createArgs, createReply)
			if createReply.ErrorCode == 0 {
				util.MasterLogf("Success in create chunk %s in server %s", handle, server)
				m.csm.AddChunk(server, handle)
				err := m.cm.ReplicateChunk(server, handle)
				if err != nil {
					return
				}
				return
			} else {
				println("damn")
				m.cm.AddHistory(handle)
			}
		}(server, handle)
	}
	time.Sleep(10 * time.Millisecond)
	return nil, handle
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, replay *gfs.MkdirReply) error {
	util.MasterLogf("Mkdir {%s}", args.Path)
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
	flag, chunk, err := m.cm.GetChunk(args.Path, args.Index)
	if err != nil {
		return err
	}
	if flag {
		err, chunk = m.createChunk(args.Path)
		if err != nil {
			return err
		}
	}
	reply.Handle = chunk
	return nil
}

func (m *Master) RPCGetLastChunkHandle(args gfs.GetLastChunkHandleArg, reply *gfs.GetLastChunkHandleReply) error {
	reply.Handle, reply.Index, _ = m.cm.GetLastChunkHandle(args.Path)
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
