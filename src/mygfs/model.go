package mygfs

import (
	"strconv"
	"time"
)

type Path string
type Offset int64
type ChunkIndex int
type ChunkHandle int64
type ChunkVersion int64
type Checksum int64

type ServerAddress struct {
	Hostname string
	Port     uint16
}

func (server *ServerAddress) ToString() string {
	return server.Hostname + ":" + strconv.FormatUint(uint64(server.Port), 10)
}

func (server *ServerAddress) IsEmpty() bool {
	return server.Port == 0 || server.Hostname == ""
}

type DataBufferID struct {
	Handle    ChunkHandle
	TimeStamp int
}

type Lease struct {
	Primary     ServerAddress
	Expire      time.Time
	Secondaries []ServerAddress
}

type PathInfo struct {
	Name string

	// if it is a directory
	IsDir bool

	// if it is a file
	Length int64
	Chunks int64
}

// Mutations

type MutationType int

const (
	MutationWrite = iota
	MutationAppend
	MutationPad
)

// chunk

type PersistentChunkInfo struct {
	Handle   ChunkHandle
	Length   Offset
	Version  ChunkVersion
	Checksum Checksum
}
