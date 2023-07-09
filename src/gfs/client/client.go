package client

import (
	"main/src/gfs"
	"main/src/gfs/util"
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
	err := util.Call(c.master, "Master.PRCCreateInfo", args, reply)
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
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {

}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	getInfoArgs := gfs.GetFileInfoArg{Path: path}
	getInfoReply := new(gfs.GetFileInfoReply)
	err := util.Call(c.master, "Master.RPCGetFileInfo", getInfoArgs, getInfoReply)
	if err != nil {
		return err
	}

}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {

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
	//args := &gfs.ReadChunkArg{
	//	Handle: handle,
	//	Offset: offset,
	//	Length: len(data),
	//}
	//reply := new(gfs.ReadChunkReply)
	//err := util.Call(c.master)
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
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
FAIL:
	pushDataArgs := gfs.PushDataAndForwardArg{
		Handle:    handle,
		Data:      data,
		ForwardTo: secondaries,
	}
	pushDataReply := new(gfs.PushDataAndForwardReply)
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
		Secondaries: secondaries,
	}
	writeReply := new(gfs.WriteChunkReply)
	util.Call(primary, "ChunkServer.RPCWriteChunk", writeArgs, writeReply)
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
	if len(data)+int(offset) > gfs.MaxChunkSize {
		return 0, gfs.Error{Code: gfs.WriteExceedChunkSize, Err: "WriteExceedChunkSize"}
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
}
