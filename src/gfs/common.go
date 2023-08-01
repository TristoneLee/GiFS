package gfs

import (
	atomic2 "github.com/uber-go/atomic"
	"strconv"
	"time"
)

type Path string
type ServerAddress string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

type MutationType int

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)

type ErrorCode int

const (
	Success = iota
	UnknownError
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
	RetryLatter
)

// extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}

var chunkCnt = atomic2.NewInt64(0)

func NewHandle() ChunkHandle {
	chunkCnt.Add(1)
	return ChunkHandle(chunkCnt.Load())
}

const (
	LeaseExpire        = 2 * time.Second //1 * time.Minute
	HeartbeatInterval  = 100 * time.Millisecond
	BackgroundInterval = 200 * time.Millisecond //
	ServerTimeout      = 1 * time.Second        //

	MaxChunkSize  = 512 << 10 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize = MaxChunkSize / 4

	DefaultNumReplicas = 3
	MinimumNumReplicas = 2

	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 10 * time.Second
)

func (p Path) String() string {
	return string(p)
}

func (s ServerAddress) String() string {
	return string(s)
}

func (c ChunkHandle) String() string {
	return strconv.FormatInt(int64(c), 10)
}

func (o Offset) String() string {
	return strconv.FormatInt(int64(o), 10)
}
