package main

import (
	"fmt"
	"gfs/src/mygfs"
	"gfs/src/mygfs/chunkserver"
	"gfs/src/mygfs/client"
	"gfs/src/mygfs/master"
	"gfs/src/mygfs/util"
	"sync"
	"time"
)

func main() {
	masterAddress := mygfs.ServerAddress{
		Hostname: "127.0.0.1",
		Port:     5400,
	}
	serverAddress := []mygfs.ServerAddress{
		{
			Hostname: "127.0.1.1",
			Port:     5401,
		},
		{
			Hostname: "127.0.1.2",
			Port:     5402,
		},
		{
			Hostname: "127.0.1.3",
			Port:     5403,
		},
		{
			Hostname: "127.0.1.4",
			Port:     5404,
		},
	}
	wg := &sync.WaitGroup{}
	// new master server
	masterNode := master.NewMaster(masterAddress, "./master")
	go masterNode.Start(wg)
	// new chunk server
	server1 := chunkserver.NewServer(serverAddress[0], masterAddress, "./server1")
	server2 := chunkserver.NewServer(serverAddress[1], masterAddress, "./server2")
	server3 := chunkserver.NewServer(serverAddress[2], masterAddress, "./server3")
	server4 := chunkserver.NewServer(serverAddress[3], masterAddress, "./server4")
	go server1.Start(wg)
	go server2.Start(wg)
	go server3.Start(wg)
	go server4.Start(wg)
	// new client
	c := client.NewClient(masterAddress)
	var err error
	time.Sleep(5 * time.Second)
	// create a file
	c.Mkdir("/dir1")
	c.Mkdir("/dir2")
	err = c.Create("/dir1/test1.txt")
	if err != nil {
		fmt.Println(err.Error())
	}
	err = c.Create("/dir1/test1.txt")
	if err != nil {
		fmt.Println(err.Error())
	}
	err = c.Create("/dir1/test2.txt")
	if err != nil {
		fmt.Println(err.Error())
	}
	err = c.Create("/dir1/test3.txt")
	if err != nil {
		fmt.Println(err.Error())
	}
	files, err := c.List("/dir1")
	fmt.Println(files)
	// append chunk
	reply, err := util.GetFileInfoCall(masterAddress, &mygfs.GetFileInfoArg{Path: "/dir1/test1.txt"})
	if err != nil {
		return
	}
	fmt.Println(*reply)

	msg := []byte("6! My Gfs NB!")
	offset, err := c.Append("/dir1/test1.txt", msg)
	if err != nil {
		return
	}
	fmt.Println("append offset", offset)
	wg.Wait()
	fmt.Println("To The End!!!")
}
