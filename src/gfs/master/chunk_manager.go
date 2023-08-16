package master

import (
	"gfsmain/src/gfs"
	"gfsmain/src/gfs/util"
	"github.com/sasha-s/go-deadlock"
	log "github.com/sirupsen/logrus"
	"time"
)

// chunkManager manges chunks
type chunkManager struct {
	deadlock.RWMutex
	chunk         map[gfs.ChunkHandle]*chunkInfo
	file          map[gfs.Path]*fileInfo
	removeHistory []gfs.ChunkHandle

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	deadlock.RWMutex
	location util.ArraySet     // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
	path     gfs.Path
	handle   gfs.ChunkHandle
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
			Err:  "",
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
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (bool, gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()
	handles, pathOk := cm.file[path]
	if !pathOk {
		return false, 0, gfs.Error{Code: 1, Err: "InvalidPath"}
	}
	if int(index) > len(handles.handles) {
		return false, 0, gfs.Error{Code: 1, Err: "InvalidIndex"}
	}
	if int(index) == len(handles.handles) {
		return true, 0, nil
	}
	handle := handles.handles[index]
	return false, handle, nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error) {
	cm.RLock()
	defer cm.RUnlock()
	info := cm.chunk[handle]
	info.Lock()
	defer info.Unlock()
	if info.location.Size() == 0 {
		return nil, gfs.Error{Code: 6, Err: "NoAvailableReplica"}
	}
	primary := info.getPrimary()
	cp := info.location.Copy()
	cp.Delete(primary)
	secondaries := cp.CastAllToServerAddress()
	return &lease{expire: info.expire, primary: primary, secondaries: secondaries}, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handles []gfs.ChunkHandle, primary gfs.ServerAddress) {
	cm.RLock()
	defer cm.RUnlock()
	for _, handle := range handles {
		info, _ := cm.chunk[handle]
		info.Lock()
		if info.primary == primary {
			info.expire = time.Now().Add(gfs.LeaseExpire)
		}
		info.Unlock()
	}
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) ReplicateChunk(addr gfs.ServerAddress, handle gfs.ChunkHandle) error {
	cm.Lock()
	defer cm.Unlock()
	cm.chunk[handle].location.Add(addr)
	return nil
}

func (cm *chunkManager) RegisterChunk(path gfs.Path, handle gfs.ChunkHandle) {
	cm.Lock()
	defer cm.Unlock()
	_, ok := cm.file[path]
	if !ok {
		cm.file[path] = &fileInfo{handles: make([]gfs.ChunkHandle, 0)}
	}
	pathInfo := cm.file[path]
	pathInfo.handles = append(pathInfo.handles, handle)
	cm.chunk[handle] = &chunkInfo{
		location: util.ArraySet{},
		primary:  "",
		expire:   time.Now().Add(-1 * gfs.LeaseExpire),
		path:     path,
		handle:   handle,
	}
}

func (cm *chunkManager) GetLastChunkHandle(path gfs.Path) (gfs.ChunkHandle, gfs.ChunkIndex, error) {
	cm.RLock()
	defer cm.RUnlock()
	info, ok := cm.file[path]
	if !ok {
		log.Fatalf("No file %s in chunk manager", path)
	}
	handles := info.handles
	index := len(handles) - 1
	return handles[index], gfs.ChunkIndex(index), nil
}

func (cm *chunkManager) ReallocateReplica(handle gfs.ChunkHandle, cur gfs.ServerAddress) error {
	util.MasterLogf("reallocate chunk %v to server %v", handle, cur)
	cm.RLock()
	info, ok := cm.chunk[handle]
	cm.RUnlock()
	if ok {
		info.Lock()
		primary := info.getPrimary()
		sendCopyArgs := &gfs.SendCopyArg{
			Handle:  handle,
			Address: cur,
		}
		sendCopyReply := new(gfs.SendCopyReply)
		info.Unlock()
		util.MasterLogf("ask server %v to send replica of chunk %v to server %v", primary, handle, cur)
		err := util.Call(primary, "ChunkServer.RPCSendCopy", sendCopyArgs, sendCopyReply)
		if err != nil {
			return err
		}
		info.Lock()
		util.MasterLogf("server %v have replica of chunk %v", cur, handle)
		info.location.Add(cur)
		info.Unlock()
		return nil
	} else {
		return gfs.Error{
			Code: 1,
			Err:  "InvalidChunkHandle",
		}
	}
}

func (cm *chunkManager) RemoveReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) {
	cm.Lock()
	defer cm.Unlock()
	info, ok := cm.chunk[handle]
	if !ok {
		log.Panicf("panic in removing chunk %v for server %v", handle, addr)
	}
	info.Lock()
	info.location.Delete(addr)
	if info.primary.String() == addr.String() {
		info.expire = time.Now().Add(-1 * gfs.LeaseExpire)
	}
	cm.removeHistory = append(cm.removeHistory, handle)
	info.Unlock()
}

func (cm *chunkManager) DetectInadequateReplica() (handles []gfs.ChunkHandle) {
	cm.Lock()
	defer cm.Unlock()
	handles = make([]gfs.ChunkHandle, len(cm.removeHistory))
	copy(handles, cm.removeHistory)
	cm.removeHistory = make([]gfs.ChunkHandle, 0)
	return handles
}

func (info *chunkInfo) getPrimary() gfs.ServerAddress {
	if time.Now().After(info.expire) {
		newPrimary_, _ := info.location.RandomPickAndGetRest()
		primary := newPrimary_.(gfs.ServerAddress)
		info.primary = primary
		util.MasterLogf("primary of chunk %v renamed as %v", info.handle, primary)
		info.expire = time.Now().Add(gfs.LeaseExpire)
	}
	return info.primary
}

func (cm *chunkManager) AddHistory(handle gfs.ChunkHandle) {
	cm.Lock()
	defer cm.Unlock()
	cm.removeHistory = append(cm.removeHistory, handle)
}
