package main

import (
	"encoding/json"
	"flag"
	"log"
	"strconv"

	"github.com/ramadhanalfarisi/headtail/src"
)

func createJobDummy() []map[string]interface{}{
	num := 1000
	jobs := []map[string]interface{}{}
	for i := 0; i < num; i++ {
		jobs = append(jobs, map[string]interface{}{
			"id": i,
			"name": "Job " + strconv.Itoa(i),
			"data": "Data " + strconv.Itoa(i),
		})
	}
	return jobs
}

func main() {
	role := flag.String("role", "master", "worker or master")	
	addr := flag.String("addr", "/tmp/master.sock", "address")
	masterAddr := flag.String("maddr", "/tmp/master.sock", "address")

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
	} else if *role == "test" {
		jobs := createJobDummy()
		for _, job := range  jobs{
			jsonJob, err := json.Marshal(job)
			if err != nil {
				log.Println("Failed to marshal job:", err)
			}
			err = src.CallRPC(*masterAddr, "MasterRPC.RegisterJob", jsonJob, nil)
			if err != nil {
				log.Println("Failed to register job:", err)
			}
			log.Println("Job registered:", job)
		}
	} else {
		panic("Invalid role")
	}
}