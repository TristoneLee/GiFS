package util

import (
	"fmt"
	"gfsmain/src/gfs"
	"time"
)

var (
	startTime = time.Now().UnixMilli()
)

func ServerLogf(s gfs.ServerAddress, format string, a ...interface{}) {
	fmt.Printf("{%8d}:[ChunkServer%s]:%s\n", time.Now().UnixMilli()-startTime, s, fmt.Sprintf(format, a...))
}

func MasterLogf(format string, a ...interface{}) {
	fmt.Printf("{%8d}:[Master]:%s\n", time.Now().UnixMilli()-startTime, fmt.Sprintf(format, a...))
}

func ClientLogf(format string, a ...interface{}) {
	fmt.Printf("{%8d}:[Client]:%s\n", time.Now().UnixMilli()-startTime, fmt.Sprintf(format, a...))
}
