package mygfs

type ErrorCode int

const (
	Success = iota
	UnknownError
	Timeout
	AppendExceedChunkSize
	WriteExceedChunkSize
	ReadEOF
	NotAvailableForCopy
	Other
)

// Error extended error type with error code
type Error struct {
	Code ErrorCode
	Err  string
}

func (e Error) Error() string {
	return e.Err
}
