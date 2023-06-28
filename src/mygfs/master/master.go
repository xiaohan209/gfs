package master

import (
	"encoding/gob"
	"fmt"
	"gfs/src/mygfs"
	"gfs/src/mygfs/util"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

// Master Server struct
type Master struct {
	address    mygfs.ServerAddress // master server address
	serverRoot string
	engine     *gin.Engine
	logger     *log.Entry
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to ture if server is shuntdown

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

const (
	MetaFileName = "gfs-master.meta"
	FilePerm     = 0755
)

func (master *Master) setupRouter() {
	router := master.engine
	router.POST("/shutdown", master.Shutdown)
	router.POST("/heartbeat", master.Heartbeat)
	router.POST("/holders", master.GetPrimaryAndSecondaries)
	router.POST("/replicas", master.GetReplicas)
	router.POST("/lease/extend", master.ExtendLease)
	router.POST("/file/create", master.CreateFile)
	router.POST("/file/delete", master.DeleteFile)
	router.POST("/file/rename", master.RenameFile)
	router.POST("/file/info", master.GetFileInfo)
	router.POST("/file/list", master.List)
	router.POST("/mkdir", master.Mkdir)
	router.POST("/chunk/handle", master.GetChunkHandle)
}

func NewMaster(address mygfs.ServerAddress, serverRoot string) *Master {
	// init master instance
	master := &Master{
		address:    address,
		serverRoot: serverRoot,
		engine:     gin.Default(),
		logger:     log.WithField("master", address.ToString()),
		shutdown:   make(chan struct{}),
		dead:       false,
	}
	// init manager
	master.nm = newNamespaceManager()
	master.cm = newChunkManager()
	master.csm = newChunkServerManager()
	// init metadata
	err := master.loadMeta()
	if err != nil {
		fmt.Println("master load meta err: ", err)
	}
	// setup routers
	master.setupRouter()
	// go func for background activity
	go master.startBackground()
	return master
}

func (master *Master) startBackground() {
	// Background Task
	// BackgroundActivity does all the background activities
	// server disconnection handle, garbage collection, stale replica detection, etc
	checkTicker := time.Tick(mygfs.ServerCheckInterval)
	storeTicker := time.Tick(mygfs.MasterStoreInterval)
	for {
		var err error
		select {
		case <-master.shutdown:
			return
		case <-checkTicker:
			err = master.serverCheck()
		case <-storeTicker:
			err = master.storeMeta()
		}
		if err != nil {
			master.logger.Error("Background error ", err)
		}
	}
}

func (master *Master) Start(group *sync.WaitGroup) {
	group.Add(1)
	err := master.engine.Run(":" + strconv.FormatUint(uint64(master.address.Port), 10))
	if err != nil {
		master.logger.Error(err.Error())
		return
	}
	group.Done()
}

type PersistentBlock struct {
	NamespaceTree []serialTreeNode
	ChunkInfo     []serialChunkInfo
}

// loadMeta loads metadata from disk
func (master *Master) loadMeta() error {
	filename := path.Join(master.serverRoot, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock
	dec := gob.NewDecoder(file)
	err = dec.Decode(&meta)
	if err != nil {
		return err
	}

	master.nm.Deserialize(meta.NamespaceTree)
	master.cm.Deserialize(meta.ChunkInfo)

	return nil
}

// storeMeta stores metadata to disk
func (master *Master) storeMeta() error {
	filename := path.Join(master.serverRoot, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var meta PersistentBlock

	meta.NamespaceTree = master.nm.Serialize()
	meta.ChunkInfo = master.cm.Serialize()

	master.logger.Infof("Master : store metadata")
	enc := gob.NewEncoder(file)
	err = enc.Encode(meta)
	return err
}

// serverCheck checks all chunkserver according to last heartbeat time
// then removes all the information of the disconnnected servers
func (master *Master) serverCheck() error {
	// detect dead servers
	addrs := master.csm.DetectDeadServers()
	for _, v := range addrs {
		master.logger.Warningf("remove server %v", v)
		handles, err := master.csm.RemoveServer(v)
		if err != nil {
			return err
		}
		err = master.cm.RemoveChunks(handles, v)
		if err != nil {
			return err
		}
	}

	// add replicas for need request
	handles := master.cm.GetNeedlist()
	if handles != nil {
		master.logger.Info("Master Need ", handles)
		master.cm.RLock()
		for i := 0; i < len(handles); i++ {
			ck := master.cm.chunk[handles[i]]

			if ck.expire.Before(time.Now()) {
				ck.Lock() // don't grant lease during copy
				err := master.reReplication(handles[i])
				master.logger.Info(err)
				ck.Unlock()
			}
		}
		master.cm.RUnlock()
	}
	return nil
}

// reReplication performs re-replication, ck should be locked in top caller
// new lease will not be granted during copy
func (master *Master) reReplication(handle mygfs.ChunkHandle) error {
	// chunk are locked, so master will not grant lease during copy time
	from, to, err := master.csm.ChooseReReplication(handle)
	if err != nil {
		return err
	}
	master.logger.Warningf("allocate new chunk %v from %v to %v", handle, from, to)

	_, err = util.CreateChunkCall(to, &mygfs.CreateChunkArg{Handle: handle})
	if err != nil {
		return err
	}
	_, err = util.SendCopyCall(from, &mygfs.SendCopyArg{Handle: handle, Address: to})
	if err != nil {
		return err
	}

	master.cm.RegisterReplica(handle, to, false)
	master.csm.AddChunk([]mygfs.ServerAddress{to}, handle)
	return nil
}
