package master

import (
	"container/ring"
	"gfsmain/src/gfs"
	"gfsmain/src/gfs/util"
	log "gfsmain/src/github.com/Sirupsen/logrus"
	"github.com/sasha-s/go-deadlock"
	"time"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	deadlock.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
	//serversCnt map[gfs.ServerAddress]int
	serverRing *ring.Ring
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers:    make(map[gfs.ServerAddress]*chunkServerInfo),
		serverRing: nil,
		//serversCnt: make(map[gfs.ServerAddress]int),
	}
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	//chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
	chunks []gfs.ChunkHandle
}

// Hearbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) {
	csm.Lock()
	defer csm.Unlock()
	serverInfo, ok := csm.servers[addr]
	if !ok {
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks:        make([]gfs.ChunkHandle, 0),
		}
		//csm.serversCnt[addr]=0;
		newRing := ring.New(1)
		newRing.Value = addr
		if csm.serverRing != nil {
			csm.serverRing.Link(newRing)
		} else {
			csm.serverRing = newRing
		}
		util.MasterLogf("Register Server:%s", addr)
	} else {
		serverInfo.lastHeartbeat = time.Now()
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addr gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()
	serverInfo, ok := csm.servers[addr]
	if !ok {
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks:        make([]gfs.ChunkHandle, 1),
		}
		csm.servers[addr].chunks[0] = handle
		log.Warn("UnexpectedInAddChunk")
		//csm.serversCnt[addr]+=1
	} else {
		serverInfo.chunks = append(serverInfo.chunks, handle)
		//csm.serversCnt[addr]+=1
	}

}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()
	flag := false
	for address, info := range csm.servers {
		for _, chunk := range info.chunks {
			if chunk == handle {
				flag = true
				break
			}
		}
		if flag {
			from = address
			break
		}
	}
	if !flag {
		return "", "", gfs.Error{
			Code: 1,
			Err:  "[Unexpected]NoAvailableReplica",
		}
	}
	to = csm.chooseServer()
	return from, to, nil
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) []gfs.ServerAddress {
	csm.Lock()
	defer csm.Unlock()
	ret := make([]gfs.ServerAddress, num)
	for i := 0; i < num; i = i + 1 {
		ret[i] = csm.chooseServer()
	}
	return ret
}

func (csm *chunkServerManager) chooseServer() (addr gfs.ServerAddress) {
	addr = csm.serverRing.Value.(gfs.ServerAddress)
	csm.serverRing = csm.serverRing.Next()
	return addr
}

// DetectDeadServers detects disconnected chunkservers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()
	ret := make([]gfs.ServerAddress, 0)
	for addr, info := range csm.servers {
		if info.lastHeartbeat.Add(gfs.ServerTimeout).Before(time.Now()) {
			ret = append(ret, addr)
		}
	}
	return ret
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) ([]gfs.ChunkHandle, error) {
	info, ok := csm.servers[addr]
	util.MasterLogf("server %v holds replicas of %v when offline", addr, info.chunks)
	handles := make([]gfs.ChunkHandle, len(info.chunks))
	copy(handles, info.chunks)
	if !ok {
		return nil, gfs.Error{
			Code: 1,
			Err:  "InvalidServerAddress",
		}
	}
	delete(csm.servers, addr)
	for i := 0; i < csm.serverRing.Len(); i = i + 1 {
		if csm.serverRing.Move(i).Value.(gfs.ServerAddress).String() == addr.String() {
			if i == 0 {
				csm.serverRing = csm.serverRing.Next()
				csm.serverRing.Move(-2).Unlink(1)
			} else {
				csm.serverRing.Move(i - 1).Unlink(1)
			}
		}
	}
	return handles, nil
}

func (csm *chunkServerManager) ReallocateChunk(handle gfs.ChunkHandle, replicas []gfs.ServerAddress) gfs.ServerAddress {
	csm.Lock()
	var server gfs.ServerAddress
	flag := true
	for flag {
		server = csm.chooseServer()
		flag = func() bool {
			for _, replica := range replicas {
				if replica == server {
					return true
				}
			}
			return false
		}()
	}
	csm.Unlock()
	csm.AddChunk(server, handle)
	return server
}
