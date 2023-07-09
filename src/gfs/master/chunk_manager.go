package master

import (
	"main/src/gfs"
	"main/src/gfs/util"
	"sync"
	"time"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location util.ArraySet     // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
	path     gfs.Path
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

type lease struct {
	primary     gfs.ServerAddress
	expire      time.Time
	secondaries []gfs.ServerAddress
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.Lock()
	defer cm.Unlock()
	info, ok := cm.chunk[handle]
	if ok {
		info.Lock()
		info.location.Add(addr)
		info.Unlock()
	} else {
		return gfs.Error{
			Code: 1,
			Err:  "InvalidChunkHandle",
		}
	}
	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (*util.ArraySet, error) {
	cm.RLock()
	defer cm.RUnlock()
	replicas, ok := cm.chunk[handle]
	if !ok {
		return nil, gfs.Error{Code: 1, Err: "InvalidChunkHandle"}
	}
	return &replicas.location, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()
	handles, pathOk := cm.file[path]
	if !pathOk {
		return 0, gfs.Error{Code: 1, Err: "InvalidPath"}
	}
	if int(index) > len(handles.handles) {
		return 0, gfs.Error{Code: 1, Err: "InvalidIndex"}
	}
	handle := handles.handles[index]
	return handle, nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error) {
	cm.RLock()
	defer cm.RUnlock()
	info := cm.chunk[handle]
	info.RLock()
	defer info.RUnlock()
	var primary gfs.ServerAddress
	var secondaries []gfs.ServerAddress
	if time.Now().After(info.expire) {
		newPrimary_, secondaries_ := info.location.RandomPickAndGetRest()
		primary = newPrimary_.(gfs.ServerAddress)
		info.primary = primary
		secondaries = secondaries_.CastAllToServerAddress()
		//todo expire confirm
		info.expire = time.Now().Add(gfs.LeaseExpire)
	} else {
		primary = info.primary
		cp := info.location.Copy()
		cp.Delete(primary)
		secondaries = cp.CastAllToServerAddress()
	}
	return &lease{expire: info.expire, primary: primary, secondaries: secondaries}, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()
	info := cm.chunk[handle]
	info.Lock()
	defer info.Unlock()
	if info.primary == primary {
		info.expire = time.Now().Add(gfs.LeaseExpire)
	}
	return nil
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, error) {
	cm.Lock()
	defer cm.Unlock()
	cm.numChunkHandle += 1
	handle := gfs.NewHandle()
	cm.file[path].handles = append(cm.file[path].handles, handle)
	newChunk := &chunkInfo{
		location: util.ArraySet{},
		primary:  "",
		expire:   time.Now(),
		path:     path,
	}
	for _, addr := range addrs {
		newChunk.location.Add(addr)
	}
	newChunk.primary = addrs[0]
	return handle, nil
}
