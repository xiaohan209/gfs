package chunkserver

import (
	"fmt"
	"gfs/src/mygfs"
	"gfs/src/mygfs/util"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"os"
	"path"
)

// ReportSelf reports all chunks the server holds
func (cs *ChunkServer) ReportSelf(con *gin.Context) {
	var reply mygfs.ReportSelfReply
	cs.lock.RLock()
	defer cs.lock.RUnlock()
	cs.logger.Debug("report collect start")
	var ret []mygfs.PersistentChunkInfo
	for handle, ck := range cs.chunk {
		//log.Info(cs.address, " report ", handle)
		persistentChunkInfo := mygfs.PersistentChunkInfo{
			Handle:   handle,
			Version:  ck.version,
			Length:   ck.length,
			Checksum: ck.checksum,
		}
		ret = append(ret, persistentChunkInfo)
	}
	reply.Chunks = ret
	cs.logger.Debug("report collect end")
	con.JSON(http.StatusOK, reply)
	return
}

func (cs *ChunkServer) Shutdown(con *gin.Context) {
	if !cs.dead {
		cs.logger.Warning("Shutdown")
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
	err := cs.storeMeta()
	if err != nil {
		cs.logger.Warning("error in store metadeta: ", err)
	}
	con.JSON(http.StatusOK, nil)
	return
}

// CheckVersion is called by master to check version ande detect stale chunk
func (cs *ChunkServer) CheckVersion(con *gin.Context) {
	var args mygfs.CheckVersionArg
	var reply mygfs.CheckVersionReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// handle chunk version function
	cs.lock.RLock()
	ck, ok := cs.chunk[args.Handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		fmt.Printf("chunk %v does not exist or is abandoned\n", args.Handle)
	}
	ck.Lock()
	defer ck.Unlock()

	if ck.version+mygfs.ChunkVersion(1) == args.Version {
		ck.version++
		reply.Stale = false
	} else {
		cs.logger.Warningf("stale chunk %v", args.Handle)
		ck.abandoned = true
		reply.Stale = true
	}
	con.JSON(http.StatusOK, reply)
	return
}

// ForwardData is called by client or another replica who sends data to the current memory buffer.
func (cs *ChunkServer) ForwardData(con *gin.Context) {
	var args mygfs.ForwardDataArg
	var reply mygfs.ForwardDataReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// check the data
	//log.Warning(cs.address, " data 1 ", args.DataID)
	if _, ok := cs.dl.Get(args.DataID); ok {
		cs.logger.Errorf("Data %v already exists", args.DataID)
	}

	cs.logger.Infof("Get data %v", args.DataID)
	//log.Warning(cs.address, "data 2 ", args.DataID)
	cs.dl.Set(args.DataID, args.Data)
	//log.Warning(cs.address, "data 3 ", args.DataID)
	// forward data with chain
	if len(args.ChainOrder) > 0 {
		next := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]
		forwardReply, err := util.ForwardDataCall(next, &args)
		if err != nil {
			cs.logger.Errorf("Data forward error -> %s", err.Error())
			con.JSON(http.StatusInternalServerError, forwardReply)
			return
		}
	}
	//log.Warning(cs.address, "data 4 ", args.DataID)
	// reply answer
	con.JSON(http.StatusOK, reply)
}

// CreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) CreateChunk(con *gin.Context) {
	var args mygfs.CreateChunkArg
	var reply mygfs.CreateChunkReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// handle create chunk function
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cs.logger.Infof("Create chunk %v", args.Handle)
	// find if there is a chunk already exist
	if _, ok := cs.chunk[args.Handle]; ok {
		cs.logger.Warning("[ignored] recreate a chunk in CreateChunk")
		con.JSON(http.StatusBadRequest, gin.H{})
		return // TODO : error handle
		//log.Errorf("Chunk %v already exists", args.Handle)
	}
	cs.chunk[args.Handle] = &chunkInfo{
		length: 0,
	}
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", args.Handle))
	_, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		cs.logger.Errorf("Openfile error")
	}
	con.JSON(http.StatusOK, reply)
	return
}

// ReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) ReadChunk(con *gin.Context) {
	var args mygfs.ReadChunkArg
	var reply mygfs.ReadChunkReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// check handle validity
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		cs.logger.Errorf("chunk %v does not exist or is abandoned", handle)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}

	// read from disk
	var err error
	reply.Data = make([]byte, args.Length)
	ck.RLock()
	reply.Length, err = cs.readChunk(handle, args.Offset, reply.Data)
	ck.RUnlock()
	if err == io.EOF {
		reply.ErrorCode = mygfs.ReadEOF
		con.JSON(http.StatusOK, reply)
		return
	}
	if err != nil {
		reply.ErrorCode = mygfs.Other
		cs.logger.Error(err.Error())
		con.JSON(http.StatusOK, reply)
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// WriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) WriteChunk(con *gin.Context) {
	var args mygfs.WriteChunkArg
	var reply mygfs.WriteChunkReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// fetch the write data
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	newLen := args.Offset + mygfs.Offset(len(data))
	if newLen > mygfs.MaxChunkSize {
		cs.logger.Errorf("writeChunk new length is too large. Size %v > MaxSize %v", len(data), mygfs.MaxChunkSize)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		cs.logger.Errorf("chunk %v does not exist or is abandoned", handle)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()
		mutation := &Mutation{mygfs.MutationWrite, data, args.Offset}

		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := mygfs.ApplyMutationArg{Mtype: mygfs.MutationWrite, DataID: args.DataID, Offset: args.Offset}
		hasError := false
		errorList := ""
		for _, secondary := range args.Secondaries {
			applyMutationReply, applyMutationErr := util.ApplyMutationCall(secondary, &callArgs)
			if applyMutationErr != nil {
				cs.logger.Error(err.Error())
				reply.ErrorCode = applyMutationReply.ErrorCode
				hasError = true
				errorList += err.Error() + ";"
			}
		}
		if hasError {
			return fmt.Errorf(errorList)
		}
		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(handle)
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// AppendChunk is called by client to apply atomic record append.
// The length of data should be within 1/4 chunk size.
// If the chunk size after appending the data will exceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) AppendChunk(con *gin.Context) {
	var args mygfs.AppendChunkArg
	var reply mygfs.AppendChunkReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// fetch the buffer
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	if len(data) > mygfs.MaxAppendSize {
		cs.logger.Errorf("append data size %v excceeds max append size %v", len(data), mygfs.MaxAppendSize)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		cs.logger.Errorf("chunk %v does not exist or is abandoned", handle)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	var mtype mygfs.MutationType

	if err = func() error {
		ck.Lock()
		defer ck.Unlock()
		newLen := ck.length + mygfs.Offset(len(data))
		offset := ck.length
		if newLen > mygfs.MaxChunkSize {
			mtype = mygfs.MutationPad
			ck.length = mygfs.MaxChunkSize
			reply.ErrorCode = mygfs.AppendExceedChunkSize
		} else {
			mtype = mygfs.MutationAppend
			ck.length = newLen
		}
		reply.Offset = offset

		mutation := &Mutation{mtype, data, offset}

		//log.Infof("Primary %v : append chunk %v version %v", cs.address, args.DataID.Handle, version)

		// apply to local
		wait := make(chan error, 1)
		go func() {
			wait <- cs.doMutation(handle, mutation)
		}()

		// call secondaries
		callArgs := mygfs.ApplyMutationArg{Mtype: mtype, DataID: args.DataID, Offset: offset}
		hasError := false
		errorList := ""
		for _, secondary := range args.Secondaries {
			applyMutationReply, applyMutationErr := util.ApplyMutationCall(secondary, &callArgs)
			if applyMutationErr != nil {
				cs.logger.Error(err.Error())
				reply.ErrorCode = applyMutationReply.ErrorCode
				hasError = true
				errorList += err.Error() + ";"
			}
		}
		if hasError {
			return fmt.Errorf(errorList)
		}
		err = <-wait
		if err != nil {
			return err
		}
		return nil
	}(); err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	// extend lease
	//cs.pendingLeaseExtensions.Add(handle)
	// reply the answer
	con.JSON(http.StatusOK, reply)
	return
}

// ApplyMutation is called by primary to apply mutations
func (cs *ChunkServer) ApplyMutation(con *gin.Context) {
	var args mygfs.ApplyMutationArg
	var reply mygfs.ApplyMutationReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// fetch
	data, err := cs.dl.Fetch(args.DataID)
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	handle := args.DataID.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		cs.logger.Errorf("cannot find chunk %v", handle)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	//log.Infof("Server %v : get mutation to chunk %v version %v", cs.address, handle, args.Version)

	mutation := &Mutation{args.Mtype, data, args.Offset}
	err = func() error {
		ck.Lock()
		defer ck.Unlock()
		err = cs.doMutation(handle, mutation)
		return err
	}()
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// SendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) SendCopy(con *gin.Context) {
	var args mygfs.SendCopyArg
	var reply mygfs.SendCopyReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		cs.logger.Error("JSON bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// check chunk validity
	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		cs.logger.Errorf("chunk %v does not exist or is abandoned", handle)
	}
	ck.RLock()
	defer ck.RUnlock()

	cs.logger.Infof("Send copy of %v to %v", handle, args.Address.ToString())
	data := make([]byte, ck.length)
	_, err := cs.readChunk(handle, 0, data)
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	applyCopyReply, err := util.ApplyCopyCall(args.Address, &mygfs.ApplyCopyArg{Handle: handle, Data: data, Version: ck.version})
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	if applyCopyReply.ErrorCode != 0 {
		reply.ErrorCode = applyCopyReply.ErrorCode
		con.JSON(http.StatusOK, reply)
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// ApplyCopy is called by another replica to rewrite the local version to given copy data
func (cs *ChunkServer) ApplyCopy(con *gin.Context) {
	var args mygfs.ApplyCopyArg
	var reply mygfs.ApplyCopyReply

	handle := args.Handle
	cs.lock.RLock()
	ck, ok := cs.chunk[handle]
	cs.lock.RUnlock()
	if !ok || ck.abandoned {
		cs.logger.Errorf("chunk %v does not exist or is abandoned", handle)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	ck.Lock()
	defer ck.Unlock()
	cs.logger.Infof("Apply copy of %v", handle)
	ck.version = args.Version
	err := cs.writeChunk(handle, args.Data, 0, true)
	if err != nil {
		cs.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	cs.logger.Infof("Apply done")
	con.JSON(http.StatusOK, reply)
	return
}
