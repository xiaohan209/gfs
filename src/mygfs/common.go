package mygfs

import "time"

// system config
const (
	// chunk
	LeaseExpire        = 3 * time.Second //1 * time.Minute
	DefaultNumReplicas = 3
	MinimumNumReplicas = 2
	MaxChunkSize       = 64 << 20 // 512KB DEBUG ONLY 64 << 20
	MaxAppendSize      = MaxChunkSize / 4
	DeletedFilePrefix  = "__del__"

	// master
	ServerCheckInterval = 400 * time.Millisecond //
	MasterStoreInterval = 30 * time.Hour         // 30 * time.Minute
	ServerTimeout       = 1 * time.Second

	// chunk server
	HeartbeatInterval    = 200 * time.Millisecond
	MutationWaitTimeout  = 4 * time.Second
	ServerStoreInterval  = 40 * time.Hour // 30 * time.Minute
	GarbageCollectionInt = 30 * time.Hour // 1 * time.Day
	DownloadBufferExpire = 2 * time.Minute
	DownloadBufferTick   = 30 * time.Second

	// client
	ClientTryTimeout = 2*LeaseExpire + 3*ServerTimeout
	LeaseBufferTick  = 500 * time.Millisecond
)
