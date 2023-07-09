package master

import (
	"main/src/gfs"
	"sync"
	"time"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers    map[gfs.ServerAddress]*chunkServerInfo
	serversCnt map[gfs.ServerAddress]int
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers:    make(map[gfs.ServerAddress]*chunkServerInfo),
		serversCnt: make(map[gfs.ServerAddress]int),
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
	csm.servers[addr].lastHeartbeat = time.Now()
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		csm.servers[addr].chunks = append(csm.servers[addr].chunks, handle)
	}
	return nil
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
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {
	csm.Lock()
	defer csm.Unlock()
	ret := make([]gfs.ServerAddress, num)
	for i := 0; i < num; i = i + 1 {
		ret[i] = csm.chooseServer()
	}
	return ret, nil
}

func (csm *chunkServerManager) chooseServer() (addr gfs.ServerAddress) {
	maxCnt := 1 << 31
	for address, cnt := range csm.serversCnt {
		if cnt < maxCnt {
			maxCnt = cnt
			addr = address
		}
	}
	csm.serversCnt[addr] += 1
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
	handles, ok := csm.servers[addr]
	if !ok {
		return nil, gfs.Error{
			Code: 1,
			Err:  "InvalidServerAddress",
		}
	}
	return handles.chunks, nil
}
