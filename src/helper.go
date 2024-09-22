package src

import (
	"net/rpc"
)

func CallRPC(srv string, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return nil
	}

	return err
}
