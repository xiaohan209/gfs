package master

import (
	"gfs/src/mygfs"
	"gfs/src/mygfs/util"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"net/http"
)

// Shutdown shuts down master
func (master *Master) Shutdown(con *gin.Context) {
	if !master.dead {
		master.logger.Warning(master.address, " Shutdown")
		master.dead = true
		close(master.shutdown)
		master.l.Close()
	}

	err := master.storeMeta()
	if err != nil {
		master.logger.Warning("error in store metadeta: ", err)
	}
}

// Heartbeat is called by chunkserver to let the master know that a chunkserver is alive
func (master *Master) Heartbeat(con *gin.Context) {
	var args mygfs.HeartbeatArg
	var reply mygfs.HeartbeatReply // TODO
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// call heartbeat
	isFirst := master.csm.Heartbeat(args.Address, &reply)

	for _, handle := range args.LeaseExtensions {
		continue // TODO
		// ATTENTION !! dead lock
		master.cm.ExtendLease(handle, args.Address)
	}

	if isFirst { // if is first heartbeat, let chunkserver report itself
		r, err := util.ReportSelfCall(args.Address, &mygfs.ReportSelfArg{})
		if err != nil {
			master.logger.Error(err.Error())
			con.JSON(http.StatusOK, gin.H{})
			return
		}

		for _, v := range r.Chunks {
			master.cm.RLock()
			ck, ok := master.cm.chunk[v.Handle]
			if !ok {
				continue
			}
			version := ck.version
			master.cm.RUnlock()

			if v.Version == version {
				master.logger.Infof("Master receive chunk %v from %v", v.Handle, args.Address)
				master.cm.RegisterReplica(v.Handle, args.Address, true)
				master.csm.AddChunk([]mygfs.ServerAddress{args.Address}, v.Handle)
			} else {
				log.Infof("Master discard %v", v.Handle)
			}
		}
	}
	// apply answer
	con.JSON(http.StatusOK, reply)
	return
}

// GetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
// Master will communicate with all replicas holder to check version, if stale replica is detected, add it to garbage collection
func (master *Master) GetPrimaryAndSecondaries(con *gin.Context) {
	var args mygfs.GetPrimaryAndSecondariesArg
	var reply mygfs.GetPrimaryAndSecondariesReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// get chunk manager lease
	lease, staleServers, err := master.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}

	for _, v := range staleServers {
		master.csm.AddGarbage(v, args.Handle)
	}

	reply.Primary = lease.Primary
	reply.Expire = lease.Expire
	reply.Secondaries = lease.Secondaries
	// apply answer
	con.JSON(http.StatusOK, reply)
	return
}

// ExtendLease extends the lease of chunk if the lessee is nobody or requester.
func (master *Master) ExtendLease(con *gin.Context) {
	var args mygfs.ExtendLeaseArg
	var reply mygfs.ExtendLeaseReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	//t, err := m.cm.ExtendLease(args.Handle, args.Address)
	//if err != nil { return err }
	//reply.Expire = *t
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// GetReplicas is called by client to find all chunkserver that holds the chunk.
func (master *Master) GetReplicas(con *gin.Context) {
	var args mygfs.GetReplicasArg
	var reply mygfs.GetReplicasReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// get replicas
	servers, err := master.cm.GetReplicas(args.Handle)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	for _, v := range servers {
		reply.Locations = append(reply.Locations, v)
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// CreateFile is called by client to create a new file
func (master *Master) CreateFile(con *gin.Context) {
	var args mygfs.CreateFileArg
	var reply mygfs.CreateFileReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// use namespace manager to create file
	err := master.nm.Create(args.Path)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// DeleteFile is called by client to delete a file
func (master *Master) DeleteFile(con *gin.Context) {
	var args mygfs.DeleteFileArg
	var reply mygfs.DeleteFileReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// use namespace manager to delete file
	err := master.nm.Delete(args.Path)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// RenameFile is called by client to rename a file
func (master *Master) RenameFile(con *gin.Context) {
	var args mygfs.RenameFileArg
	var reply mygfs.RenameFileReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// use name space manager to rename file
	err := master.nm.Rename(args.Source, args.Target)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// Mkdir is called by client to make a new directory
func (master *Master) Mkdir(con *gin.Context) {
	var args mygfs.MkdirArg
	var reply mygfs.MkdirReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// use name space manager to make dir
	err := master.nm.Mkdir(args.Path)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// List is called by client to list all files in specific directory
func (master *Master) List(con *gin.Context) {
	var args mygfs.ListArg
	var reply mygfs.ListReply
	var err error
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// use name space manager to list files
	reply.Files, err = master.nm.List(args.Path)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// GetFileInfo is called by client to get file information
func (master *Master) GetFileInfo(con *gin.Context) {
	var args mygfs.GetFileInfoArg
	var reply mygfs.GetFileInfoReply
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	// use name space manager to get file info
	ps, cwd, err := master.nm.lockParents(args.Path, false)
	defer master.nm.unlockParents(ps)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// look up file
	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		master.logger.Errorf("File %v does not exist", args.Path)
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	file.Lock()
	defer file.Unlock()

	reply.IsDir = file.isDir
	reply.Length = file.length
	reply.Chunks = file.chunks
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by one, create one.
func (master *Master) GetChunkHandle(con *gin.Context) {
	var args mygfs.GetChunkHandleArg
	var reply *mygfs.GetChunkHandleReply //TODO
	// parse json args
	jsonErr := con.ShouldBindJSON(&args)
	if jsonErr != nil {
		master.logger.Error("json bind error", jsonErr)
		con.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	//
	ps, cwd, err := master.nm.lockParents(args.Path, false)
	defer master.nm.unlockParents(ps)
	if err != nil {
		master.logger.Error(err.Error())
		con.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// append new chunks
	file, ok := cwd.children[ps[len(ps)-1]]
	if !ok {
		master.logger.Errorf("File %v does not exist", args.Path)
		con.JSON(http.StatusInternalServerError, gin.H{})
	}
	file.Lock()
	defer file.Unlock()
	if int(args.Index) == int(file.chunks) {
		file.chunks++
		addrs, err := master.csm.ChooseServers(mygfs.DefaultNumReplicas)
		if err != nil {
			master.logger.Error(err.Error())
			con.JSON(http.StatusInternalServerError, gin.H{})
			return
		}

		reply.Handle, addrs, err = master.cm.CreateChunk(args.Path, addrs)
		if err != nil {
			// WARNING
			master.logger.Warning("[ignored] An ignored error in GetChunkHandle when create ", err, " in create chunk ", reply.Handle)
		}

		master.csm.AddChunk(addrs, reply.Handle)
	} else {
		reply.Handle, err = master.cm.GetChunk(args.Path, args.Index)
	}
	// reply answer
	con.JSON(http.StatusOK, reply)
	return
}
