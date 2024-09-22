package main

import (
	"encoding/json"
	"flag"
	"log"

	"github.com/ramadhanalfarisi/headtail/src"
)

func main() {
	role := flag.String("role", "master", "worker or master")	
	addr := flag.String("addr", "/tmp/master.sock", "address")
	masterAddr := flag.String("maddr", "/tmp/master.sock", "address")

	jobs := []map[string]interface{}{
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
		{
			"head": "1",
			"tail": "1",
			"result": "1",
		},
		{
			"head": "1",
			"tail": "2",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "1",
			"result": "3",
		},
		{
			"head": "2",
			"tail": "2",
			"result": "4",
		},
	}


	flag.Parse()

	if *role == "master" {
		master := src.NewMaster(*addr)
		master.Listen()
	} else if *role == "worker" {
		worker := src.NewWorker(*addr, *masterAddr, func(job []byte) error {
			log.Println("Job received:", string(job))
			return nil
		})
		worker.Listen()
	} else if *role == "main" {
		for _, job := range jobs {
			jsonJob, err := json.Marshal(job)
			if err != nil {
				log.Println("Failed to marshal job:", err)
			}
			err = src.CallRPC(*masterAddr, "MasterRPC.RegisterJob", jsonJob, nil)
			if err != nil {
				log.Println("Failed to register job:", err)
			}
		}
	} else {
		panic("Invalid role")
	}
}