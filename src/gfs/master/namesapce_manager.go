package master

import (
	"gfsmain/src/gfs"
	"github.com/sasha-s/go-deadlock"
	log "github.com/sirupsen/logrus"
	"strings"
)

type namespaceManager struct {
	root *nsTree
}

type nsTree struct {
	deadlock.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	//length int64
	//chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{
			RWMutex:  deadlock.RWMutex{},
			isDir:    true,
			children: make(map[string]*nsTree),
		},
	}
	return nm
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	pArgs := strings.Split(string(p), "/")
	pArgs = pArgs[1:]
	return nm.innerCreate(nm.root, pArgs)
}

func (nm *namespaceManager) innerCreate(dir *nsTree, pArgs []string) error {
	dir.RLock()
	defer dir.RWMutex.RUnlock()
	arg := pArgs[0]
	pArgs = pArgs[1:]
	nxtDir, ok := dir.children[arg]
	if len(pArgs) == 0 {
		if ok {
			log.Error("Double create file")
			return gfs.Error{
				Code: 1,
				Err:  "DoubleCreateFile",
			}
		} else {
			dir.children[arg] = &nsTree{isDir: false}
			return nil
		}
	} else {
		if !ok {
			dir.children[arg] = &nsTree{isDir: true, children: make(map[string]*nsTree)}
			nxtDir = dir.children[arg]
		}
		return nm.innerCreate(nxtDir, pArgs)
	}
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	pArgs := strings.Split(string(p), "/")
	pArgs = pArgs[1:]
	return nm.innerMkdir(nm.root, pArgs)
}

func (nm *namespaceManager) innerMkdir(dir *nsTree, pArgs []string) error {
	dir.RLock()
	defer dir.RWMutex.RUnlock()
	arg := pArgs[0]
	pArgs = pArgs[1:]
	nxtDir, ok := dir.children[arg]
	if len(pArgs) == 0 {
		if ok {
			log.Error("Double mkdir")
			return gfs.Error{
				Code: 1,
				Err:  "DoubleMkdir",
			}
		} else {
			dir.children[arg] = &nsTree{isDir: true, children: make(map[string]*nsTree)}
			return nil
		}
	} else {
		if !ok {
			dir.children[arg] = &nsTree{isDir: true, children: make(map[string]*nsTree)}
			nxtDir = dir.children[arg]
		}
		return nm.innerMkdir(nxtDir, pArgs)
	}
}

func (nm *namespaceManager) QueryDir(p gfs.Path) (files []gfs.PathInfo, err error) {
	pArgs := strings.Split(string(p), "/")
	for len(pArgs) != 0 && pArgs[0] == "" {
		pArgs = pArgs[1:]
	}
	ptr := nm.root
	for _, arg := range pArgs {
		ptr.RLock()
		nxtPtr, ok := ptr.children[arg]
		if !ok {
			ptr.RUnlock()
			return nil, gfs.Error{
				Code: 1,
				Err:  "InvalidPathInQueryDir",
			}
		}
		ptr.RUnlock()
		ptr = nxtPtr
	}
	if !ptr.isDir {
		return nil, gfs.Error{
			Code: 1,
			Err:  "InvalidPathInQueryDir",
		}
	}
	ret := make([]gfs.PathInfo, 0)
	ptr.RLock()
	defer ptr.RUnlock()
	for name, tree := range ptr.children {
		tree.RLock()
		ret = append(ret, gfs.PathInfo{
			Name:  name,
			IsDir: tree.isDir,
			//Length: tree.length,
			//Chunks: tree.chunks,
		})
		tree.RUnlock()
	}
	return ret, nil
}

func (nm *namespaceManager) QueryFile(p gfs.Path) (info gfs.PathInfo, err error) {
	pArgs := strings.Split(string(p), "/")
	ptr := nm.root
	for _, arg := range pArgs {
		ptr.RLock()
		nxtPtr, ok := ptr.children[arg]
		if !ok {
			ptr.RUnlock()
			return gfs.PathInfo{}, gfs.Error{
				Code: 1,
				Err:  "InvalidPathInQueryFile",
			}
		}
		ptr.RUnlock()
		ptr = nxtPtr
	}
	if ptr.isDir {
		return gfs.PathInfo{}, gfs.Error{
			Code: 1,
			Err:  "InvalidPathInQueryFile",
		}
	}
	ptr.RLock()
	defer ptr.RUnlock()
	return gfs.PathInfo{
		Name:  pArgs[len(pArgs)-1],
		IsDir: false,
		//Length: ptr.length,
		//Chunks: ptr.chunks,
	}, nil
}
