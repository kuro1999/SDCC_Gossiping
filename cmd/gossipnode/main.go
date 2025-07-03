package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"SDCC_gossiping/gossip"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: gossipnode <id> <bindAddr> <peer1,peer2,...>")
		return
	}
	id := os.Args[1]
	bind := os.Args[2]
	peers := strings.Split(os.Args[3], ",")

	node, err := gossip.Config(id, bind, 2*time.Second, peers)
	if err != nil {
		panic(err)
	}
	if err := node.Start(); err != nil {
		panic(err)
	}

	select {} // blocca il main
}
