package client

import (
	"fmt"
	"gfs/src/mygfs"
	"gfs/src/mygfs/chunkserver"
	"gfs/src/mygfs/util"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"time"
)

// Client struct is the GFS client-side driver
type Client struct {
	master   mygfs.ServerAddress
	leaseBuf *leaseBuffer
}

// NewClient returns a new gfs client.
func NewClient(master mygfs.ServerAddress) *Client {
	return &Client{
		master:   master,
		leaseBuf: newLeaseBuffer(master, mygfs.LeaseBufferTick),
	}
}

// Create is a client API, creates a file
func (c *Client) Create(path mygfs.Path) error {
	_, err := util.CreateFileCall(c.master, &mygfs.CreateFileArg{Path: path})
	if err != nil {
		return err
	}
	return nil
}

// Delete is a client API, deletes a file
func (c *Client) Delete(path mygfs.Path) error {
	_, err := util.DeleteFileCall(c.master, &mygfs.DeleteFileArg{Path: path})
	if err != nil {
		return err
	}
	return nil
}

// Rename is a client API, deletes a file
func (c *Client) Rename(source mygfs.Path, target mygfs.Path) error {
	_, err := util.RenameFileCall(c.master, &mygfs.RenameFileArg{Source: source, Target: target})
	if err != nil {
		return err
	}
	return nil
}

// Mkdir is a client API, makes a directory
func (c *Client) Mkdir(path mygfs.Path) error {
	_, err := util.MkdirCall(c.master, &mygfs.MkdirArg{Path: path})
	if err != nil {
		return err
	}
	return nil
}

// List is a client API, lists all files in specific directory
func (c *Client) List(path mygfs.Path) ([]mygfs.PathInfo, error) {
	reply, err := util.ListCall(c.master, &mygfs.ListArg{Path: path})
	if err != nil {
		return nil, err
	}
	return reply.Files, nil
}

// Read is a client API, read file at specific offset
// it reads up to len(data) bytes form the File. it returns the number of bytes and an error.
// the error is set to io.EOF if stream meets the end of file
func (c *Client) Read(path mygfs.Path, offset mygfs.Offset, data []byte) (n int, err error) {
	reply, err := util.GetFileInfoCall(c.master, &mygfs.GetFileInfoArg{Path: path})
	if err != nil {
		return -1, err
	}

	if int64(offset/mygfs.MaxChunkSize) > reply.Chunks {
		return -1, fmt.Errorf("read offset exceeds file size")
	}

	pos := 0
	for pos < len(data) {
		index := mygfs.ChunkIndex(offset / mygfs.MaxChunkSize)
		chunkOffset := offset % mygfs.MaxChunkSize

		if int64(index) >= reply.Chunks {
			err = mygfs.Error{Code: mygfs.ReadEOF, Err: "EOF over chunks"}
			break
		}

		var handle mygfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, index)
		if err != nil {
			return
		}

		var n int
		//wait := time.NewTimer(gfs.ClientTryTimeout)
		//loop:
		for {
			//select {
			//case <-wait.C:
			//    err = gfs.MyGFSError{gfs.Timeout, "Read Timeout"}
			//    break loop
			//default:
			//}
			n, err = c.ReadChunk(handle, chunkOffset, data[pos:])
			if err == nil || err.(mygfs.Error).Code == mygfs.ReadEOF {
				break
			}
			log.Warning("Read ", handle, " connection error, try again: ", err)
		}

		offset += mygfs.Offset(n)
		pos += n
		if err != nil {
			break
		}
	}

	if err != nil && err.(mygfs.Error).Code == mygfs.ReadEOF {
		return pos, io.EOF
	} else {
		return pos, err
	}
}

// Write is a client API. write data to file at specific offset
func (c *Client) Write(path mygfs.Path, offset mygfs.Offset, data []byte) error {
	reply, err := util.GetFileInfoCall(c.master, &mygfs.GetFileInfoArg{Path: path})
	if err != nil {
		return err
	}

	if int64(offset/mygfs.MaxChunkSize) > reply.Chunks {
		return fmt.Errorf("write offset exceeds file size")
	}

	begin := 0
	for {
		index := mygfs.ChunkIndex(offset / mygfs.MaxChunkSize)
		chunkOffset := offset % mygfs.MaxChunkSize

		handle, err := c.GetChunkHandle(path, index)
		if err != nil {
			return err
		}

		writeMax := int(mygfs.MaxChunkSize - chunkOffset)
		var writeLen int
		if begin+writeMax > len(data) {
			writeLen = len(data) - begin
		} else {
			writeLen = writeMax
		}

		//wait := time.NewTimer(mygfs.ClientTryTimeout)
		//loop:
		for {
			//select {
			//case <-wait.C:
			//    err = fmt.Errorf("Write Timeout")
			//    break loop
			//default:
			//}
			err = c.WriteChunk(handle, chunkOffset, data[begin:begin+writeLen])
			if err == nil {
				break
			}
			log.Warning("Write ", handle, "  connection error, try again ", err)
		}
		if err != nil {
			return err
		}

		offset += mygfs.Offset(writeLen)
		begin += writeLen

		if begin == len(data) {
			break
		}
	}
	return nil
}

// Append is a client API, append data to file
func (c *Client) Append(path mygfs.Path, data []byte) (offset mygfs.Offset, err error) {
	if len(data) > mygfs.MaxAppendSize {
		return 0, fmt.Errorf("len(data) = %v > max append size %v", len(data), mygfs.MaxAppendSize)
	}

	reply, err := util.GetFileInfoCall(c.master, &mygfs.GetFileInfoArg{Path: path})
	if err != nil {
		return
	}

	start := mygfs.ChunkIndex(reply.Chunks - 1)
	if start < 0 {
		start = 0
	}

	var chunkOffset mygfs.Offset
	for {
		var handle mygfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, start)
		if err != nil {
			return
		}

		//wait := time.NewTimer(mygfs.ClientTryTimeout)
		//loop:
		for {
			//select {
			//case <-wait.C:
			//	err = mygfs.MyGFSError{mygfs.Timeout, "Append Timeout"}
			//	break loop
			//default:
			//}
			chunkOffset, err = c.AppendChunk(handle, data)
			if err == nil || err.(mygfs.Error).Code == mygfs.AppendExceedChunkSize {
				break
			}
			log.Warning("Append ", handle, " connection error, try again ", err)
			time.Sleep(50 * time.Millisecond)
		}
		if err == nil || err.(mygfs.Error).Code != mygfs.AppendExceedChunkSize {
			break
		}

		// retry in next chunk
		start++
		log.Info("pad this, try on next chunk ", start)
	}

	if err != nil {
		return
	}

	offset = mygfs.Offset(start)*mygfs.MaxChunkSize + chunkOffset
	return
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path mygfs.Path, index mygfs.ChunkIndex) (mygfs.ChunkHandle, error) {
	reply, err := util.GetChunkHandleCall(c.master, &mygfs.GetChunkHandleArg{Path: path, Index: index})
	if err != nil {
		return 0, err
	}
	return reply.Handle, nil
}

// ReadChunk read data from the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) ReadChunk(handle mygfs.ChunkHandle, offset mygfs.Offset, data []byte) (int, error) {
	var readLen int

	if mygfs.MaxChunkSize-offset > mygfs.Offset(len(data)) {
		readLen = len(data)
	} else {
		readLen = int(mygfs.MaxChunkSize - offset)
	}

	getReplicasReply, err := util.GetReplicasCall(c.master, &mygfs.GetReplicasArg{Handle: handle})
	if err != nil {
		return 0, mygfs.Error{Code: mygfs.UnknownError, Err: err.Error()}
	}
	loc := getReplicasReply.Locations[rand.Intn(len(getReplicasReply.Locations))]
	if len(getReplicasReply.Locations) == 0 {
		return 0, mygfs.Error{Code: mygfs.UnknownError, Err: "no replica"}
	}

	readChunkReply, err := util.ReadChunkCall(loc, &mygfs.ReadChunkArg{Handle: handle, Offset: offset, Length: readLen})
	if err != nil {
		return 0, mygfs.Error{Code: mygfs.UnknownError, Err: err.Error()}
	}
	readChunkReply.Data = data
	if readChunkReply.ErrorCode == mygfs.ReadEOF {
		return readChunkReply.Length, mygfs.Error{Code: mygfs.ReadEOF, Err: "read EOF"}
	}
	return readChunkReply.Length, nil
}

// WriteChunk writes data to the chunk at specific offset.
// <code>len(data)+offset</data> should be within chunk size.
func (c *Client) WriteChunk(handle mygfs.ChunkHandle, offset mygfs.Offset, data []byte) error {
	if len(data)+int(offset) > mygfs.MaxChunkSize {
		return fmt.Errorf("len(data)+offset = %v > max chunk size %v", len(data)+int(offset), mygfs.MaxChunkSize)
	}

	l, err := c.leaseBuf.Get(handle)
	if err != nil {
		return err
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Secondaries, l.Primary)

	_, err = util.ForwardDataCall(chain[0], &mygfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]})
	if err != nil {
		return err
	}

	wcargs := mygfs.WriteChunkArg{DataID: dataID, Offset: offset, Secondaries: l.Secondaries}
	_, err = util.WriteChunkCall(l.Primary, &wcargs)
	return err
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// <code>len(data)</code> should be within 1/4 chunk size.
func (c *Client) AppendChunk(handle mygfs.ChunkHandle, data []byte) (offset mygfs.Offset, err error) {
	if len(data) > mygfs.MaxAppendSize {
		return 0, mygfs.Error{Code: mygfs.UnknownError, Err: fmt.Sprintf("len(data) = %v > max append size %v", len(data), mygfs.MaxAppendSize)}
	}

	//log.Infof("Client : get lease ")

	l, err := c.leaseBuf.Get(handle)
	if err != nil {
		return -1, mygfs.Error{Code: mygfs.UnknownError, Err: err.Error()}
	}

	dataID := chunkserver.NewDataID(handle)
	chain := append(l.Secondaries, l.Primary)

	//log.Warning("Client : get locations %v", chain)
	_, err = util.ForwardDataCall(chain[0], &mygfs.ForwardDataArg{DataID: dataID, Data: data, ChainOrder: chain[1:]})
	if err != nil {
		return -1, mygfs.Error{Code: mygfs.UnknownError, Err: err.Error()}
	}

	//log.Warning("Client : send append request to primary. data : %v", dataID)

	acargs := mygfs.AppendChunkArg{DataID: dataID, Secondaries: l.Secondaries}
	appendReply, err := util.AppendChunkCall(l.Primary, &acargs)
	if err != nil {
		return -1, mygfs.Error{Code: mygfs.UnknownError, Err: err.Error()}
	}
	if appendReply.ErrorCode == mygfs.AppendExceedChunkSize {
		return appendReply.Offset, mygfs.Error{Code: appendReply.ErrorCode, Err: "append over chunks"}
	}
	return appendReply.Offset, nil
}
