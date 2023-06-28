package master

import (
	"fmt"
	"gfs/src/mygfs"
	"gfs/src/mygfs/util"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[mygfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[mygfs.ServerAddress]*chunkServerInfo),
	}
	log.Info("-----------new chunk server manager")
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[mygfs.ChunkHandle]bool // set of chunks that the chunkserver has
	garbage       []mygfs.ChunkHandle
}

func (csm *chunkServerManager) Heartbeat(addr mygfs.ServerAddress, reply *mygfs.HeartbeatReply) bool {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if !ok {
		log.Info("New chunk server" + addr.ToString())
		csm.servers[addr] = &chunkServerInfo{time.Now(), make(map[mygfs.ChunkHandle]bool), nil}
		return true
	} else {
		// send garbage
		reply.Garbage = csm.servers[addr].garbage
		csm.servers[addr].garbage = make([]mygfs.ChunkHandle, 0)
		sv.lastHeartbeat = time.Now()
		return false
	}
}

// AddChunk register a chunk to servers
func (csm *chunkServerManager) AddChunk(addrs []mygfs.ServerAddress, handle mygfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	for _, v := range addrs {
		//csm.servers[v].chunks[handle] = true
		sv, ok := csm.servers[v]
		if ok {
			sv.chunks[handle] = true
		} else {
			log.Warning("add chunk in removed server ", sv)
		}
	}
}

// AddGarbage add some chunk handle to garbage
func (csm *chunkServerManager) AddGarbage(addr mygfs.ServerAddress, handle mygfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()

	sv, ok := csm.servers[addr]
	if ok {
		sv.garbage = append(sv.garbage, handle)
	}
}

// ChooseReReplication chooses servers to perfomr re-replication
// called when the replicas number of a chunk is less than mygfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle mygfs.ChunkHandle) (from, to mygfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = mygfs.ServerAddress{
		Hostname: "",
		Port:     0,
	}
	to = mygfs.ServerAddress{
		Hostname: "",
		Port:     0,
	}
	err = nil
	for a, v := range csm.servers {
		if v.chunks[handle] {
			from = a
		} else {
			to = a
		}
		if !from.IsEmpty() && !to.IsEmpty() {
			return
		}
	}
	err = fmt.Errorf("no enough server for replica %v", handle)
	return
}

// ChooseServers returns servers to store new chunk
// called when a new chunk is created
func (csm *chunkServerManager) ChooseServers(num int) ([]mygfs.ServerAddress, error) {

	if num > len(csm.servers) {
		return nil, fmt.Errorf("no enough servers for %v replicas", num)
	}

	csm.RLock()
	var all, ret []mygfs.ServerAddress
	for a, _ := range csm.servers {
		all = append(all, a)
	}
	csm.RUnlock()

	choose, err := util.Sample(len(all), num)
	if err != nil {
		return nil, err
	}
	for _, v := range choose {
		ret = append(ret, all[v])
	}

	return ret, nil
}

// DetectDeadServers detect disconnected servers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []mygfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()

	var ret []mygfs.ServerAddress
	now := time.Now()
	for k, v := range csm.servers {
		if v.lastHeartbeat.Add(mygfs.ServerTimeout).Before(now) {
			ret = append(ret, k)
		}
	}

	return ret
}

// RemoveServer removes metedata of disconnected server
// it returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr mygfs.ServerAddress) (handles []mygfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	err = nil
	sv, ok := csm.servers[addr]
	if !ok {
		err = fmt.Errorf("cannot find chunk server %v", addr)
		return
	}
	for h, v := range sv.chunks {
		if v {
			handles = append(handles, h)
		}
	}
	delete(csm.servers, addr)

	return
}
