package mygfs

import (
	"time"
)

// Check version
type CheckVersionArg struct {
	Handle  ChunkHandle
	Version ChunkVersion
}
type CheckVersionReply struct {
	Stale bool
}

// chunk IO
type ForwardDataArg struct {
	DataID     DataBufferID
	Data       []byte
	ChainOrder []ServerAddress
}
type ForwardDataReply struct {
	ErrorCode ErrorCode
}

type CreateChunkArg struct {
	Handle ChunkHandle
}
type CreateChunkReply struct {
	ErrorCode ErrorCode
}

type WriteChunkArg struct {
	DataID      DataBufferID
	Offset      Offset
	Secondaries []ServerAddress
}
type WriteChunkReply struct {
	ErrorCode ErrorCode
}

type AppendChunkArg struct {
	DataID      DataBufferID
	Secondaries []ServerAddress
}
type AppendChunkReply struct {
	Offset    Offset
	ErrorCode ErrorCode
}

type ApplyMutationArg struct {
	Mtype  MutationType
	DataID DataBufferID
	Offset Offset
}
type ApplyMutationReply struct {
	ErrorCode ErrorCode
}

type PadChunkArg struct {
	Handle ChunkHandle
}
type PadChunkReply struct {
	ErrorCode ErrorCode
}

type ReadChunkArg struct {
	Handle ChunkHandle
	Offset Offset
	Length int
}
type ReadChunkReply struct {
	Data      []byte
	Length    int
	ErrorCode ErrorCode
}

// re-replication
type SendCopyArg struct {
	Handle  ChunkHandle
	Address ServerAddress
}
type SendCopyReply struct {
	ErrorCode ErrorCode
}

type ApplyCopyArg struct {
	Handle  ChunkHandle
	Data    []byte
	Version ChunkVersion
}
type ApplyCopyReply struct {
	ErrorCode ErrorCode
}

// no use argument
type Nouse struct{}

/*
 *  Master
 */

// handshake
type HeartbeatArg struct {
	Address          ServerAddress `json:"ServerAddress"`    // chunkserver address
	LeaseExtensions  []ChunkHandle `json:"LeaseExtensions"`  // leases to be extended
	AbandondedChunks []ChunkHandle `json:"AbandondedChunks"` // unrecoverable chunks
}
type HeartbeatReply struct {
	Garbage []ChunkHandle `json:"Garbage"`
}

type ReportSelfArg struct {
}
type ReportSelfReply struct {
	Chunks []PersistentChunkInfo `json:"Chunks"`
}

// chunk info
type GetPrimaryAndSecondariesArg struct {
	Handle ChunkHandle `json:"Handle"`
}
type GetPrimaryAndSecondariesReply struct {
	Primary     ServerAddress   `json:"Primary"`
	Expire      time.Time       `json:"Expire"`
	Secondaries []ServerAddress `json:"Secondaries"`
}

type ExtendLeaseArg struct {
	Handle  ChunkHandle   `json:"Handle"`
	Address ServerAddress `json:"Address"`
}
type ExtendLeaseReply struct {
	Expire time.Time `json:"Expire"`
}

type GetReplicasArg struct {
	Handle ChunkHandle `json:"Handle"`
}
type GetReplicasReply struct {
	Locations []ServerAddress `json:"Locations"`
}

type GetFileInfoArg struct {
	Path Path `json:"Path"`
}
type GetFileInfoReply struct {
	IsDir  bool  `json:"IsDir"`
	Length int64 `json:"Length"`
	Chunks int64 `json:"Chunks"`
}

type GetChunkHandleArg struct {
	Path  Path       `json:"Path"`
	Index ChunkIndex `json:"Index"`
}
type GetChunkHandleReply struct {
	Handle ChunkHandle `json:"Handle"`
}

// namespace operation
type CreateFileArg struct {
	Path Path `json:"Path"`
}
type CreateFileReply struct{}

type DeleteFileArg struct {
	Path Path `json:"Path"`
}
type DeleteFileReply struct{}

type RenameFileArg struct {
	Source Path `json:"Source"`
	Target Path `json:"Target"`
}
type RenameFileReply struct{}

type MkdirArg struct {
	Path Path `json:"Path"`
}
type MkdirReply struct{}

type ListArg struct {
	Path Path `json:"Path"`
}
type ListReply struct {
	Files []PathInfo `json:"Files"`
}

// shutdown

type ShutdownArg struct {
}

type ShutdownReply struct {
}
