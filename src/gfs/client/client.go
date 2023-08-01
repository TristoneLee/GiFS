package client

import (
	"gfsmain/src/gfs"
	"gfsmain/src/gfs/util"
	log "gfsmain/src/github.com/Sirupsen/logrus"

	"github.com/huandu/go-clone"
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	c := &Client{
		master: master,
	}
	return c
}

// Create creates a new file on the specific path on GFS.
func (c *Client) Create(path gfs.Path) error {
	args := &gfs.CreateFileArg{Path: path}
	reply := &gfs.CreateFileReply{}
	err := util.Call(c.master, "Master.RPCCreateFile", args, reply)
	if err != nil {
		return err
	}
	return nil
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	args := &gfs.MkdirArg{Path: path}
	reply := &gfs.MkdirReply{}
	err := util.Call(c.master, "Master.PRCMkdir", args, reply)
	if err != nil {
		return err
	}
	return nil
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	args := gfs.ListArg{Path: path}
	reply := new(gfs.ListReply)
	err := util.Call(c.master, "Master.PRCList", args, reply)
	if err != nil {
		return nil, err
	}
	return reply.Files, err
}

// Read reads the file at specific offset.
//// It reads up to len(data) bytes form the File.
//// It return the number of bytes, and an error if any.
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (int, error) {
	starts, ends := chunkCutting(offset, int64(len(data)))
	startChunk := gfs.ChunkIndex(int64(offset) / gfs.MaxChunkSize)
	endChunk := gfs.ChunkIndex((offset + gfs.Offset(len(data))) / gfs.MaxChunkSize)
	chunkCnt := int(endChunk-startChunk) + 1
	//resultCh := make(chan error, chunkCnt)
	dataPtr := 0
	for i := 0; i < chunkCnt; i = i + 1 {
		dataSlice := make([]byte, int(ends[i]-starts[i]))
		getChunkArgs := &gfs.GetChunkHandleArg{Path: path, Index: gfs.ChunkIndex(i + int(startChunk))}
		getChunkReply := new(gfs.GetChunkHandleReply)
		err := util.Call(c.master, "Master.RPCGetChunkHandle", getChunkArgs, getChunkReply)
		if err != nil {
			//log.Panic("unexpected error in read")
			return 0, err
		}
		_, err = c.ReadChunk(getChunkReply.Handle, starts[i], dataSlice)
		if err != nil {
			return 0, err
		}
		for j := 0; j < len(dataSlice); j = j + 1 {
			data[j+dataPtr] = dataSlice[j]
		}
		dataPtr = dataPtr + int(ends[i]-starts[i])
	}
	//chCnt := 0
	//for err := range resultCh {
	//	if err != nil {
	//	}
	//	chCnt += 1
	//	if chCnt == cap(resultCh) {
	//		break
	//	}
	//}
	// util.ClientLogf("%v", data)
	return len(data), nil
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	starts, ends := chunkCutting(offset, int64(len(data)))
	startChunk := gfs.ChunkIndex(int64(offset) / gfs.MaxChunkSize)
	endChunk := gfs.ChunkIndex((offset + gfs.Offset(len(data))) / gfs.MaxChunkSize)
	chunkCnt := int(endChunk-startChunk) + 1
	resultCh := make(chan error, chunkCnt)
	dataPtr := 0
	for i := 0; i < chunkCnt; i = i + 1 {
		func(index int, offset gfs.Offset, data []byte) {
			err := func() error {
				getChunkArgs := &gfs.GetChunkHandleArg{Path: path, Index: gfs.ChunkIndex(index)}
				getChunkReply := new(gfs.GetChunkHandleReply)
				getChunkErr := util.Call(c.master, "Master.RPCGetChunkHandle", getChunkArgs, getChunkReply)
				if getChunkErr != nil {

					return getChunkErr
				}
				return c.WriteChunk(getChunkReply.Handle, offset, data)
			}()
			resultCh <- err
		}(i+int(startChunk),
			starts[i],
			data[dataPtr:dataPtr+int(ends[i]-starts[i])])
		dataPtr = dataPtr + int(ends[i]-starts[i])
	}
	chCnt := 0
	for err := range resultCh {
		if err != nil {
			return err
		}
		chCnt += 1
		if chCnt == cap(resultCh) {
			break
		}
	}
	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	getInfoArgs := gfs.GetLastChunkHandleArg{Path: path}
	getInfoReply := new(gfs.GetLastChunkHandleReply)
	err = util.Call(c.master, "Master.RPCGetLastChunkHandle", getInfoArgs, getInfoReply)
	if err != nil {
		return 0, err
	}
	handle := getInfoReply.Handle
	index := getInfoReply.Index
	failCnt := 0
	//util.ClientLogf("initial index %v", index)
REDO:
	offset, err = c.AppendChunk(handle, data)
	for err != nil && err.(gfs.Error).Code == gfs.AppendExceedChunkSize {
		index += 1
		handle, err = c.GetChunkHandle(path, index)
		if err != nil {
			return 0, err
		}
		offset, err = c.AppendChunk(handle, data)
		if err != nil {
			return 0, err
		}
	}
	if err != nil {
		if failCnt >= 10 {
			log.Fatal("fail to append")
		}
		failCnt += 1
		goto REDO
	}
	//util.ClientLogf("offset %v index %v", offset, index)
	return gfs.Offset(int64(offset) + int64(index)*int64(gfs.MaxChunkSize)), err
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	args := &gfs.GetChunkHandleArg{Path: path, Index: index}
	reply := new(gfs.GetChunkHandleReply)
	err := util.Call(c.master, "Master.RPCGetChunkHandle", args, reply)
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	getPriSecArgs := gfs.GetPrimaryAndSecondariesArg{Handle: handle}
	getPriSecReply := new(gfs.GetPrimaryAndSecondariesReply)
	getPriSecErr := util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", getPriSecArgs, getPriSecReply)
	if getPriSecErr != nil {
		return 0, getPriSecErr
	}
	primary := getPriSecReply.Primary
	readArgs := &gfs.ReadChunkArg{
		Handle: handle,
		Offset: offset,
		Length: len(data),
	}
	readReply := new(gfs.ReadChunkReply)
	util.ClientLogf("read %v bytes chunk %v at offset %v", len(data), handle, offset)
	readErr := util.Call(primary, "ChunkServer.RPCReadChunk", readArgs, readReply)
	if readErr != nil {
		return 0, readErr
	}
	copy(data, readReply.Data)
	//util.ClientLogf("get data %v", data)
	return len(data), nil
}

// WriteChunk writes data to the chunk at specific offset.
//// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	util.ClientLogf("write chunk %v at offset %v of length %v", handle, offset, len(data))
	failCnt := 0
REDO:
	if len(data)+int(offset) > gfs.MaxChunkSize {
		return gfs.Error{Code: gfs.WriteExceedChunkSize, Err: "WriteExceedChunkSize"}
	}
	getPriSecArgs := gfs.GetPrimaryAndSecondariesArg{Handle: handle}
	getPriSecReply := new(gfs.GetPrimaryAndSecondariesReply)
	util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", getPriSecArgs, getPriSecReply)
	primary := getPriSecReply.Primary
	secondaries := getPriSecReply.Secondaries
	if len(secondaries) == 0 {
		log.Warn("should have secondaries for chunk %v", handle)
	}
FAIL:
	pushDataArgs := gfs.PushDataAndForwardArg{
		Handle:    handle,
		Data:      data,
		ForwardTo: clone.Clone(secondaries).([]gfs.ServerAddress),
	}
	pushDataReply := new(gfs.PushDataAndForwardReply)
	util.ClientLogf("Call PRCPushData to %s", primary)
	util.Call(primary, "ChunkServer.RPCPushDataAndForward", pushDataArgs, pushDataReply)
	if pushDataReply.ErrorCode != 0 {
		return gfs.Error{
			Code: pushDataReply.ErrorCode,
			Err:  "",
		}
	}
	dataID := pushDataReply.DataID
	writeArgs := gfs.WriteChunkArg{
		Handle:      handle,
		DataID:      dataID,
		Offset:      offset,
		Secondaries: clone.Clone(secondaries).([]gfs.ServerAddress),
	}
	writeReply := new(gfs.WriteChunkReply)
	err := util.Call(primary, "ChunkServer.RPCWriteChunk", writeArgs, writeReply)
	if err != nil {
		return err
	}
	if writeReply.ErrorCode != 0 {
		failCnt += 1
		if failCnt > 5 {
			goto REDO
		}
		goto FAIL
	}
	return nil
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	if len(data) > gfs.MaxAppendSize {
		return 0, gfs.Error{
			Code: gfs.UnknownError,
			Err:  "AppendExceedMaxAppendSize",
		}
	}
	getPriSecArgs := gfs.GetPrimaryAndSecondariesArg{Handle: handle}
	getPriSecReply := new(gfs.GetPrimaryAndSecondariesReply)
	util.Call(c.master, "Master.RPCGetPrimaryAndSecondaries", getPriSecArgs, getPriSecReply)
	primary := getPriSecReply.Primary
	secondaries := getPriSecReply.Secondaries
	pushDataArgs := gfs.PushDataAndForwardArg{
		Handle:    handle,
		Data:      data,
		ForwardTo: secondaries,
	}
	pushDataReply := new(gfs.PushDataAndForwardReply)
	util.Call(primary, "ChunkServer.RPCPushDataAndForward", pushDataArgs, pushDataReply)
	if pushDataReply.ErrorCode != 0 {
		return 0, gfs.Error{
			Code: pushDataReply.ErrorCode,
			Err:  "",
		}
	}
	dataID := pushDataReply.DataID
	appendArgs := gfs.AppendChunkArg{
		Handle:      handle,
		DataID:      dataID,
		Secondaries: secondaries,
	}
	appendReply := new(gfs.AppendChunkReply)
	err = util.Call(primary, "ChunkServer.RPCAppendChunk", appendArgs, appendReply)
	if err != nil {
		return 0, gfs.Error{Code: gfs.UnknownError, Err: "Unknown error in appendchunk"}
	}
	if appendReply.ErrorCode == 0 {
		return appendReply.Offset, nil
	}
	return gfs.MaxChunkSize, gfs.Error{Code: gfs.AppendExceedChunkSize}
}

func chunkCutting(offset gfs.Offset, length int64) ([]gfs.Offset, []gfs.Offset) {
	start := int64(offset) / gfs.MaxChunkSize
	end := (int64(offset) + length) / gfs.MaxChunkSize
	starts := make([]gfs.Offset, end-start+1)
	ends := make([]gfs.Offset, end-start+1)
	for i := start; i <= end; i = i + 1 {
		starts[i-start] = maxOffset(offset-gfs.Offset(i*gfs.MaxChunkSize), 0)
		ends[i-start] = minOffset(gfs.MaxChunkSize, offset+gfs.Offset(length)-gfs.Offset(i*gfs.MaxChunkSize))
	}
	return starts, ends
}

func maxOffset(rhs gfs.Offset, lhs gfs.Offset) gfs.Offset {
	if rhs >= lhs {
		return rhs
	}
	return lhs
}

func minOffset(rhs gfs.Offset, lhs gfs.Offset) gfs.Offset {
	if rhs >= lhs {
		return lhs
	}
	return rhs
}
