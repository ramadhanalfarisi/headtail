package src

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)


type WorkerRPC struct {
	addr string
	jobCallback func(job []byte) error
	masterAddr string
	listener net.Listener
	shutdown chan bool
}

func NewWorker(addr string, masterAddr string, jobCallback func(job []byte) error) *WorkerRPC {
	worker := &WorkerRPC{
		addr: addr,
		masterAddr: masterAddr,
		jobCallback: jobCallback,
	}
	worker.shutdown = make(chan bool)
	return worker
}

func (m *WorkerRPC) createRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)

	socketPath := m.addr

	os.RemoveAll(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}
	m.listener = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Failed to accept connection:", err)
				continue
			}
			go rpcs.ServeConn(conn)
		}
	}()

}

func (m *WorkerRPC) Register() {
	err := CallRPC(m.masterAddr, "MasterRPC.RegisterWorker", m.addr, nil)
	if err != nil {
		log.Println("Failed to register worker:", err)
		time.Sleep(5 * time.Second)
		m.shutdown <- true
	}
}

func (m *WorkerRPC) DoJob(job []byte, _ *struct{}) error {
	err := m.jobCallback(job)
	return err
}

func (m *WorkerRPC) Listen() {
	m.createRPCServer()
	m.Register()
	
	<-m.shutdown
}



