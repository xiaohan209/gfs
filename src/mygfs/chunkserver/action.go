package chunkserver

import (
	"encoding/gob"
	"fmt"
	"gfs/src/mygfs"
	"gfs/src/mygfs/util"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"path"
)

// heartbeat calls master with chunkserver status
func (cs *ChunkServer) heartbeat() error {
	pe := cs.pendingLeaseExtensions.GetAllAndClear()
	le := make([]mygfs.ChunkHandle, len(pe))
	for i, v := range pe {
		le[i] = v.(mygfs.ChunkHandle)
	}
	args := &mygfs.HeartbeatArg{
		Address:         cs.address,
		LeaseExtensions: le,
	}
	heartBeatReply, err := util.HeartBeatCall(cs.master, args)
	if err != nil {
		return err
	}
	cs.garbage = append(cs.garbage, heartBeatReply.Garbage...)
	return err
}

// garbageCollection collect garbage in this chunkserver
func (cs *ChunkServer) garbageCollection() error {
	for _, v := range cs.garbage {
		cs.deleteChunk(v)
	}

	cs.garbage = make([]mygfs.ChunkHandle, 0)
	return nil
}

// loadMeta loads metadata from disk
func (cs *ChunkServer) loadMeta() error {
	cs.lock.Lock()
	defer cs.lock.Unlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_RDONLY, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []mygfs.PersistentChunkInfo
	dec := gob.NewDecoder(file)
	err = dec.Decode(&metas)
	if err != nil {
		return err
	}

	log.Infof("Server %v : load metadata len: %v", cs.address, len(metas))

	// load into memory
	for _, ck := range metas {
		//log.Infof("Server %v restore %v version: %v length: %v", cs.address, ck.Handle, ck.Version, ck.Length)
		cs.chunk[ck.Handle] = &chunkInfo{
			length:  ck.Length,
			version: ck.Version,
		}
	}

	return nil
}

// storeMeta stores metadate to disk
func (cs *ChunkServer) storeMeta() error {
	cs.lock.RLock()
	defer cs.lock.RUnlock()

	filename := path.Join(cs.rootDir, MetaFileName)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []mygfs.PersistentChunkInfo
	for handle, ck := range cs.chunk {
		metas = append(metas, mygfs.PersistentChunkInfo{
			Handle: handle, Length: ck.length, Version: ck.version,
		})
	}

	log.Infof("Server %v : store metadata len: %v", cs.address, len(metas))
	enc := gob.NewEncoder(file)
	err = enc.Encode(metas)

	return err
}

// writeChunk writes data at offset to a chunk at disk
func (cs *ChunkServer) writeChunk(handle mygfs.ChunkHandle, data []byte, offset mygfs.Offset, lock bool) error {
	cs.lock.RLock()
	ck := cs.chunk[handle]
	cs.lock.RUnlock()

	// ck is already locked in top util
	newLen := offset + mygfs.Offset(len(data))
	if newLen > ck.length {
		ck.length = newLen
	}

	if newLen > mygfs.MaxChunkSize {
		log.Fatal("new length > gfs.MaxChunkSize")
	}

	log.Infof("Server %v : write to chunk %v at %v len %v", cs.address, handle, offset, len(data))
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, FilePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteAt(data, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

// readChunk reads data at offset from a chunk at dist
func (cs *ChunkServer) readChunk(handle mygfs.ChunkHandle, offset mygfs.Offset, data []byte) (int, error) {
	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))

	f, err := os.Open(filename)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	log.Infof("Server %v : read chunk %v at %v len %v", cs.address, handle, offset, len(data))
	return f.ReadAt(data, int64(offset))
}

// deleteChunk deletes a chunk during garbage collection
func (cs *ChunkServer) deleteChunk(handle mygfs.ChunkHandle) error {
	cs.lock.Lock()
	delete(cs.chunk, handle)
	cs.lock.Unlock()

	filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", handle))
	err := os.Remove(filename)
	return err
}

// apply mutations (write, append, pad) in chunk buffer in proper order according to version number
func (cs *ChunkServer) doMutation(handle mygfs.ChunkHandle, m *Mutation) error {
	// already locked
	var lock bool
	if m.mtype == mygfs.MutationAppend {
		lock = true
	} else {
		lock = false
	}

	var err error
	if m.mtype == mygfs.MutationPad {
		data := []byte{0}
		err = cs.writeChunk(handle, data, mygfs.MaxChunkSize-1, lock)
	} else {
		err = cs.writeChunk(handle, m.data, m.offset, lock)
	}

	if err != nil {
		cs.lock.RLock()
		ck := cs.chunk[handle]
		cs.lock.RUnlock()
		log.Warningf("%v abandon chunk %v", cs.address, handle)
		ck.abandoned = true
		return err
	}

	return nil
}

// padChunk pads a chunk to max chunk size.
// <code>cs.chunk[handle]</code> should be locked in advance
func (cs *ChunkServer) padChunk(handle mygfs.ChunkHandle, version mygfs.ChunkVersion) error {
	log.Fatal("ChunkServer.padChunk is deserted!")
	//ck := cs.chunk[handle]
	//ck.version = version
	//ck.length = gfs.MaxChunkSize
	return nil
}

// =================== DEBUG TOOLS ===================
func getContents(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close() // f.Close will run when we're finished.

	var result []byte
	buf := make([]byte, 100)
	for {
		n, err := f.Read(buf[0:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err // f will be closed if we return here.
		}
		result = append(result, buf[0:n]...) // append is discussed later.
	}
	return string(result), nil // f will be closed if we return here.
}

func (cs *ChunkServer) PrintSelf(no1 mygfs.Nouse, no2 *mygfs.Nouse) error {
	cs.lock.RLock()
	cs.lock.RUnlock()
	log.Info("============ ", cs.address, " ============")
	if cs.dead {
		log.Warning("DEAD")
	} else {
		for h, v := range cs.chunk {
			filename := path.Join(cs.rootDir, fmt.Sprintf("chunk%v.chk", h))
			log.Infof("chunk %v : version %v", h, v.version)
			str, _ := getContents(filename)
			log.Info(str)
		}
	}
	return nil
}
