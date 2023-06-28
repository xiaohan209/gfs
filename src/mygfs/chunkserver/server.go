package chunkserver

import (
	"gfs/src/mygfs"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	MetaFileName = "gfs-server.meta"
	FilePerm     = 0755
)

// ChunkServer struct
type ChunkServer struct {
	lock     sync.RWMutex
	address  mygfs.ServerAddress // chunkserver address
	master   mygfs.ServerAddress // master address
	rootDir  string              // path to data storage
	engine   *gin.Engine
	logger   *log.Entry
	l        net.Listener
	shutdown chan struct{}

	dl                     *downloadBuffer                  // expiring download buffer
	chunk                  map[mygfs.ChunkHandle]*chunkInfo // chunk information
	dead                   bool                             // set to ture if server is shutdown
	pendingLeaseExtensions *mygfs.ArraySet                  // pending lease extension
	garbage                []mygfs.ChunkHandle
}

type Mutation struct {
	mtype  mygfs.MutationType
	data   []byte
	offset mygfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length    mygfs.Offset
	version   mygfs.ChunkVersion // version number of the chunk in disk
	checksum  mygfs.Checksum
	mutations map[mygfs.ChunkVersion]*Mutation // mutation buffer
	abandoned bool                             // unrecoverable error
}

func (cs *ChunkServer) setupRouter() {
	router := cs.engine
	router.POST("/report", cs.ReportSelf)
	router.POST("/shutdown", cs.Shutdown)
	router.POST("/version/check", cs.CheckVersion)
	router.POST("/data/forward", cs.ForwardData)
	router.POST("/chunk/create", cs.CreateChunk)
	router.POST("/chunk/read", cs.ReadChunk)
	router.POST("/chunk/write", cs.WriteChunk)
	router.POST("/chunk/append", cs.AppendChunk)
	router.POST("/mutation/apply", cs.ApplyMutation)
	router.POST("/copy/send", cs.SendCopy)
	router.POST("/copy/apply", cs.ApplyCopy)
}

func NewServer(localAddress mygfs.ServerAddress, masterAddress mygfs.ServerAddress, rootDir string) *ChunkServer {
	// new server instance
	chunkServer := &ChunkServer{
		lock:                   sync.RWMutex{},
		address:                localAddress,
		master:                 masterAddress,
		rootDir:                rootDir,
		engine:                 gin.Default(),
		logger:                 log.WithField("server", localAddress.ToString()),
		shutdown:               make(chan struct{}),
		dl:                     newDownloadBuffer(mygfs.DownloadBufferExpire, mygfs.DownloadBufferTick),
		chunk:                  make(map[mygfs.ChunkHandle]*chunkInfo),
		dead:                   false,
		pendingLeaseExtensions: new(mygfs.ArraySet),
		garbage:                make([]mygfs.ChunkHandle, 0),
	}
	// Mkdir for root dir
	_, err := os.Stat(rootDir)
	if err != nil { // not exist
		err := os.Mkdir(rootDir, FilePerm)
		if err != nil {
			log.Fatal("error in mkdir ", err)
		}
	}
	// load metadata for recovery
	err = chunkServer.loadMeta()
	if err != nil {
		log.Warning("Error in load metadata: ", err)
	}
	// setup routers
	chunkServer.setupRouter()
	// Background Activity
	go chunkServer.startBackground()
	// log out starting status
	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", localAddress, rootDir, masterAddress)
	return chunkServer
}

func (cs *ChunkServer) startBackground() {
	// Background Activity
	// heartbeat, store persistent meta, garbage collection ...
	heartbeatTicker := time.Tick(mygfs.HeartbeatInterval)
	storeTicker := time.Tick(mygfs.ServerStoreInterval)
	garbageTicker := time.Tick(mygfs.GarbageCollectionInt)
	quickStart := make(chan bool, 1) // send first heartbeat right away..
	quickStart <- true
	for {
		var err error
		var branch string
		select {
		case <-cs.shutdown:
			return
		case <-quickStart:
			branch = "heartbeat"
			err = cs.heartbeat()
		case <-heartbeatTicker:
			branch = "heartbeat"
			err = cs.heartbeat()
		case <-storeTicker:
			branch = "storemeta"
			err = cs.storeMeta()
		case <-garbageTicker:
			branch = "garbagecollecton"
			err = cs.garbageCollection()
		}

		if err != nil {
			log.Errorf("%v background(%v) error %v", cs.address, branch, err)
		}
	}
}

func (cs *ChunkServer) Start(group *sync.WaitGroup) {
	group.Add(1)
	err := cs.engine.Run(":" + strconv.FormatUint(uint64(cs.address.Port), 10))
	if err != nil {
		cs.logger.Error(err.Error())
		return
	}
	group.Done()
}
