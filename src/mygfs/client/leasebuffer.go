package client

import (
	"gfs/src/mygfs"
	"gfs/src/mygfs/util"
	"sync"
	"time"
)

type leaseBuffer struct {
	sync.RWMutex
	master mygfs.ServerAddress
	buffer map[mygfs.ChunkHandle]*mygfs.Lease
	tick   time.Duration
}

// newLeaseBuffer returns a leaseBuffer.
// The downloadBuffer will cleanup expired items every tick.
func newLeaseBuffer(ms mygfs.ServerAddress, tick time.Duration) *leaseBuffer {
	buf := &leaseBuffer{
		buffer: make(map[mygfs.ChunkHandle]*mygfs.Lease),
		tick:   tick,
		master: ms,
	}

	// cleanup
	go func() {
		ticker := time.Tick(tick)
		for {
			<-ticker
			now := time.Now()
			buf.Lock()
			for id, item := range buf.buffer {
				if item.Expire.Before(now) {
					delete(buf.buffer, id)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *leaseBuffer) Get(handle mygfs.ChunkHandle) (*mygfs.Lease, error) {
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]

	if !ok { // ask master to send one
		holdersReply, err := util.GetPrimaryAndSecondaries(buf.master, &mygfs.GetPrimaryAndSecondariesArg{Handle: handle})
		if err != nil {
			return nil, err
		}

		lease = &mygfs.Lease{Primary: holdersReply.Primary, Expire: holdersReply.Expire, Secondaries: holdersReply.Secondaries}
		buf.buffer[handle] = lease
		return lease, nil
	}
	// extend lease (it is the work of chunk server)
	/*
	   go func() {
	       var r gfs.ExtendLeaseReply
	       util.Call(buf.master, "Master.RPCExtendLease", gfs.ExtendLeaseArg{handle, lease.Primary}, &r)
	       lease.Expire = r.Expire
	   }()
	*/
	return lease, nil
}
