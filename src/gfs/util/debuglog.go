package util

import (
	"time"
)

var (
	startTime = time.Now()
)

func ServerLog(s string, info string) {
	println(":[INFO]Server:", s, " || ")
}
