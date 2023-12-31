package chunkserver

import (
	"fmt"
	"gfs/src/mygfs"
	"sync"
	"time"
)

type downloadItem struct {
	data   []byte
	expire time.Time
}

type downloadBuffer struct {
	sync.RWMutex
	buffer map[mygfs.DataBufferID]downloadItem
	expire time.Duration
	tick   time.Duration
}

// newDownloadBuffer returns a downloadBuffer. Default expire time is expire.
// The downloadBuffer will cleanup expired items every tick.
func newDownloadBuffer(expire, tick time.Duration) *downloadBuffer {
	buf := &downloadBuffer{
		buffer: make(map[mygfs.DataBufferID]downloadItem),
		expire: expire,
		tick:   tick,
	}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

// NewDataID allocate a new DataID for given handle
func NewDataID(handle mygfs.ChunkHandle) mygfs.DataBufferID {
	now := time.Now()
	timeStamp := now.Nanosecond() + now.Second()*1000 + now.Minute()*60*1000
	return mygfs.DataBufferID{Handle: handle, TimeStamp: timeStamp}
}

func (buf *downloadBuffer) Set(id mygfs.DataBufferID, data []byte) {
	buf.Lock()
	defer buf.Unlock()
	buf.buffer[id] = downloadItem{data, time.Now().Add(buf.expire)}
}

func (buf *downloadBuffer) Get(id mygfs.DataBufferID) ([]byte, bool) {
	buf.Lock()
	defer buf.Unlock()
	item, ok := buf.buffer[id]
	if !ok {
		return nil, ok
	}
	item.expire = time.Now().Add(buf.expire) // touch
	return item.data, ok
}

func (buf *downloadBuffer) Fetch(id mygfs.DataBufferID) ([]byte, error) {
	buf.Lock()
	defer buf.Unlock()

	item, ok := buf.buffer[id]
	if !ok {
		return nil, fmt.Errorf("DataID %v not found in download buffer.", id)
	}

	delete(buf.buffer, id)
	return item.data, nil
}

func (buf *downloadBuffer) Delete(id mygfs.DataBufferID) {
	buf.Lock()
	defer buf.Unlock()
	delete(buf.buffer, id)
}
