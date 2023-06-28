package util

import (
	"bytes"
	"encoding/json"
	"gfs/src/mygfs"
	"io"
	"net/http"
)

//// Call is RPC call helper
//func Call(srv mygfs.ServerAddress, rpcname string, args interface{}, reply interface{}) error {
//	c, errx := rpc.Dial("tcp", string(srv))
//	if errx != nil {
//		return errx
//	}
//	defer c.Close()
//
//	err := c.Call(rpcname, args, reply)
//	return err
//}

// ApplyCopyCall ChunkServer.ApplyCopy
func ApplyCopyCall(server mygfs.ServerAddress, arg *mygfs.ApplyCopyArg) (*mygfs.ApplyCopyReply, error) {
	var reply = new(mygfs.ApplyCopyReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/copy/apply"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// SendCopyCall ChunkServer.RPCSendCopy
func SendCopyCall(server mygfs.ServerAddress, arg *mygfs.SendCopyArg) (*mygfs.SendCopyReply, error) {
	var reply = new(mygfs.SendCopyReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/copy/send"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// ApplyMutationCall ChunkServer.ApplyMutation call for ChunkServer to other to apply mutation
func ApplyMutationCall(server mygfs.ServerAddress, arg *mygfs.ApplyMutationArg) (*mygfs.ApplyMutationReply, error) {
	var reply = new(mygfs.ApplyMutationReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/mutation/apply"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// ReadChunkCall ChunkServer.ReadChunk for client to read chunk
func ReadChunkCall(server mygfs.ServerAddress, arg *mygfs.ReadChunkArg) (*mygfs.ReadChunkReply, error) {
	var reply = new(mygfs.ReadChunkReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/chunk/read"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// ForwardDataCall ChunkServer.ForwardData
func ForwardDataCall(server mygfs.ServerAddress, arg *mygfs.ForwardDataArg) (*mygfs.ForwardDataReply, error) {
	var reply = new(mygfs.ForwardDataReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/data/forward"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// CreateChunkCall ChunkServer.CreateChunk
func CreateChunkCall(server mygfs.ServerAddress, arg *mygfs.CreateChunkArg) (*mygfs.CreateChunkReply, error) {
	var reply = new(mygfs.CreateChunkReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/chunk/create"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// WriteChunkCall ChunkServer.WriteChunk
func WriteChunkCall(server mygfs.ServerAddress, arg *mygfs.WriteChunkArg) (*mygfs.WriteChunkReply, error) {
	var reply = new(mygfs.WriteChunkReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/chunk/write"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// AppendChunkCall ChunkServer.AppendChunk
func AppendChunkCall(server mygfs.ServerAddress, arg *mygfs.AppendChunkArg) (*mygfs.AppendChunkReply, error) {
	var reply = new(mygfs.AppendChunkReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/chunk/append"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// ReportSelfCall ChunkServer.ReportSelf
func ReportSelfCall(server mygfs.ServerAddress, arg *mygfs.ReportSelfArg) (*mygfs.ReportSelfReply, error) {
	var reply = new(mygfs.ReportSelfReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/report"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// CheckVersionCall ChunkServer.CheckVersion
func CheckVersionCall(server mygfs.ServerAddress, arg *mygfs.CheckVersionArg) (*mygfs.CheckVersionReply, error) {
	var reply = new(mygfs.CheckVersionReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/version/check"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// ===============Master======================

// HeartBeatCall Master.Heartbeat
func HeartBeatCall(server mygfs.ServerAddress, arg *mygfs.HeartbeatArg) (*mygfs.HeartbeatReply, error) {
	var reply = new(mygfs.HeartbeatReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/heartbeat"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// CreateFileCall Master.CreateFile
func CreateFileCall(server mygfs.ServerAddress, arg *mygfs.CreateFileArg) (*mygfs.CreateFileReply, error) {
	var reply = new(mygfs.CreateFileReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/file/create"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// DeleteFileCall Master.DeleteFile
func DeleteFileCall(server mygfs.ServerAddress, arg *mygfs.DeleteFileArg) (*mygfs.DeleteFileReply, error) {
	var reply = new(mygfs.DeleteFileReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/file/delete"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// RenameFileCall Master.RenameFile
func RenameFileCall(server mygfs.ServerAddress, arg *mygfs.RenameFileArg) (*mygfs.RenameFileReply, error) {
	var reply = new(mygfs.RenameFileReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/file/rename"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// MkdirCall Master.Mkdir call to make dir
func MkdirCall(server mygfs.ServerAddress, arg *mygfs.MkdirArg) (*mygfs.MkdirReply, error) {
	var reply = new(mygfs.MkdirReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/mkdir"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// ListCall Master.List call to list file in dir
func ListCall(server mygfs.ServerAddress, arg *mygfs.ListArg) (*mygfs.ListReply, error) {
	var reply = new(mygfs.ListReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/file/list"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// GetFileInfoCall Master.GetFileInfo
func GetFileInfoCall(server mygfs.ServerAddress, arg *mygfs.GetFileInfoArg) (*mygfs.GetFileInfoReply, error) {
	var reply = new(mygfs.GetFileInfoReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/file/info"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// GetChunkHandleCall Master.GetChunkHandle
func GetChunkHandleCall(server mygfs.ServerAddress, arg *mygfs.GetChunkHandleArg) (*mygfs.GetChunkHandleReply, error) {
	var reply = new(mygfs.GetChunkHandleReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/chunk/handle"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

// GetReplicasCall Master.GetReplicas
func GetReplicasCall(server mygfs.ServerAddress, arg *mygfs.GetReplicasArg) (*mygfs.GetReplicasReply, error) {
	var reply = new(mygfs.GetReplicasReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/file/list"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}

func GetPrimaryAndSecondaries(server mygfs.ServerAddress, arg *mygfs.GetPrimaryAndSecondariesArg) (*mygfs.GetPrimaryAndSecondariesReply, error) {
	var reply = new(mygfs.GetPrimaryAndSecondariesReply)
	client := &http.Client{}
	// json turns to bytes
	argBytes, jsonMarshalErr := json.Marshal(*arg)
	if jsonMarshalErr != nil {
		return nil, jsonMarshalErr
	}
	// post
	url := "http://" + server.ToString() + "/holders"
	req, requestErr := http.NewRequest("POST", url, bytes.NewBuffer(argBytes))
	if requestErr != nil {
		return nil, requestErr
	}
	resp, respErr := client.Do(req)
	if respErr != nil {
		return nil, respErr
	}
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	jsonUnmarshalErr := json.Unmarshal(body, reply)
	if jsonUnmarshalErr != nil {
		return nil, jsonUnmarshalErr
	}
	return reply, nil
}
