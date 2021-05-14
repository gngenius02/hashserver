package main

import (
	"log"
	"net"
	"net/http"

	// _ "net/http/pprof"
	"startgoserver/common"
	"startgoserver/redis"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

var epoller *epoll

func wsHandler(w http.ResponseWriter, r *http.Request) {
	if conn, _, _, err := ws.UpgradeHTTP(r, w); err != nil {
		return
	} else {
		if err := epoller.Add(conn); err != nil {
			log.Printf("Failed to add connection %v", err)
			conn.Close()
		}
	}
}

func main() {
	// Increase resources limitations
	// var rLimit syscall.Rlimit
	// if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
	// 	panic(err)
	// }
	// rLimit.Cur = rLimit.Max
	// if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
	// 	panic(err)
	// }

	// // Enable pprof hooks
	// go func() {
	// 	if err := http.ListenAndServe("localhost:6060", nil); err != nil {
	// 		log.Fatalf("pprof failed: %v", err)
	// 	}
	// }()

	// Start epoll
	var err error
	if epoller, err = MkEpoll(); err != nil {
		panic(err)
	} else {
		go Start()
		http.HandleFunc("/", wsHandler)
		if err := http.ListenAndServe("0.0.0.0:3000", nil); err != nil {
			log.Fatal(err)
		}
	}
}

func RecvMessageHandler(conn net.Conn, msg []byte) {
	length := 1000000 + 1

	h := make(common.HashArray, length)
	h[0] = string(msg)

	h.GetHashes()
	h.TransformBase64()

	go redis.GetData(h, conn)
}

func Start() {
	log.Printf("Server started. Listening for websocket connections on port 3000\n")

	for {
		if connections, err := epoller.Wait(); err != nil {
			// log.Printf("Failed to epoll wait %v\n", err)
			continue
		} else {
			for _, conn := range connections {
				if conn == nil {
					break
				}
				if msg, _, err := wsutil.ReadClientData(conn); err != nil {
					if err := epoller.Remove(conn); err != nil {
						log.Printf("Failed to remove %v\n", err)
					}
					conn.Close()
				} else {
					// RECEIVE MESSAGE FROM CLIENT
					// log.Printf("msg: %s\n", string(msg))
					go RecvMessageHandler(conn, msg)
				}
			}
		}
	}
}
