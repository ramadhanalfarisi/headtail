package src

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterRPC struct {
	mu       sync.Mutex
	muJob    sync.Mutex
	addr     string
	workers  []string
	cond     *sync.Cond
	condJob  *sync.Cond
	listener net.Listener
	jobs     [][]byte
}

func NewMaster(addr string) *MasterRPC {
	master := &MasterRPC{
		addr: addr,
	}
	master.cond = sync.NewCond(&master.mu)
	master.condJob = sync.NewCond(&master.muJob)
	return master
}

func (m *MasterRPC) Listen() {
	availableWorker := make(chan string)
	availableJob := make(chan []byte)
	shutdown := make(chan struct{})

	m.createRPCServer()
	m.scheduleWorker(availableWorker)
	m.scheduleJob(availableJob)
	m.doJob(availableWorker, availableJob)

	<-shutdown
}

func (m *MasterRPC) SendJob(args []byte) error {
	err := CallRPC(m.addr, "MasterRPC.RegisterJob", args, nil)
	if err != nil {
		return err
	}
	return nil
}

func (m *MasterRPC) createRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)

	socketPath := m.addr
	os.RemoveAll(socketPath) // Remove previous socket file

	listener, err := net.Listen("unix", socketPath) // Create a new listener on the Unix socket
	if err != nil {
		panic(err)
	}
	m.listener = listener

	go func() {
		defer m.listener.Close() // Ensure listener is closed properly when the goroutine exits
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Println("Failed to accept connection:", err)
				return // Break or return, depending on how you want to handle errors
			}

			// Handle each connection in a separate goroutine
			go func(conn net.Conn) {
				defer conn.Close() // Ensure connection is closed properly after use
				rpcs.ServeConn(conn)
			}(conn) // Pass the connection object to avoid closure capture issues
		}
	}()
}

func (m *MasterRPC) RegisterWorker(addr string, _ *struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Println("Worker", addr, "registered")
	m.workers = append(m.workers, addr)
	m.cond.Broadcast()
	return nil
}

func (m *MasterRPC) RegisterJob(args []byte, _ *struct{}) error {
	m.muJob.Lock()
	defer m.muJob.Unlock()
	m.jobs = append(m.jobs, args)
	m.condJob.Broadcast()
	return nil
}

func (m *MasterRPC) doJob(availableWorker chan string, availableJob chan []byte) {
	go func() {
		time.Sleep(15 * time.Second)

		m.mu.Lock()         // Lock access to shared data
		defer m.mu.Unlock() // Ensure unlock happens at the end of the function

		if len(m.workers) == 0 {
			log.Println("No available worker")
		} else if len(m.jobs) == 0 {
			log.Println("No available job")
		}
	}()

	for {
		m.mu.Lock() // Lock access to shared data
		if len(m.workers) == 0 || len(m.jobs) == 0 {
			m.mu.Unlock() // Unlock before continuing the loop
			continue
		}

		worker := <-availableWorker
		job := <-availableJob

		m.mu.Unlock() // Unlock after accessing shared resources

		// Handle job in a separate goroutine
		go func(worker string, job []byte) {
			isSuccess := false
			numRetry := 0

			for !isSuccess {
				err := CallRPC(worker, "WorkerRPC.DoJob", job, nil)
				if err != nil {
					numRetry++
					if numRetry == 3 {
						log.Println("Retry job failed")
						isSuccess = true
					}
					log.Println("Retry job")
					time.Sleep(1 * time.Second)
				} else {
					isSuccess = true // Mark job as successful if there are no errors
				}
			}
			availableWorker <- worker // Return the worker to the channel
		}(worker, job) // Pass worker and job explicitly to avoid closure capture issues
	}
}

func (m *MasterRPC) scheduleWorker(availableWorker chan string) {
	go func() {
		i := 0
		for {
			m.mu.Lock()
			for len(m.workers) <= i {
				m.cond.Wait()
			}

			log.Println("Worker", m.workers[i], "available")
			go func(i int) { availableWorker <- m.workers[i] }(i)
			i++

			m.mu.Unlock()
		}
	}()
}

func (m *MasterRPC) scheduleJob(availableJob chan []byte) {
	go func() {
		i := 0
		for {
			m.muJob.Lock()
			for len(m.jobs) <= i {
				m.condJob.Wait()
			}
			go func(i int) { availableJob <- m.jobs[i] }(i)
			i++

			m.muJob.Unlock()
		}
	}()
}
