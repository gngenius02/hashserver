package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hashServer/shardmap"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type Server struct {
	epoller *epoll
	port    string
	m       *shardmap.Map
	saveLoc string
}

var s *Server

func InitServer(m *shardmap.Map, dataLoc string) {
	port := os.Getenv("WEBSOCKET_PORT")
	if port == "" {
		port = "3000"
	}
	epoller, err := MkEpoll()

	s = &Server{epoller: epoller, port: port, m: m, saveLoc: dataLoc}

	if err != nil {
		panic(err)
	} else {
		go s.listenForConnections()
		http.HandleFunc("/", s.wsHandler)
		if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
			log.Fatal(err)
		}
	}
}

func (s *Server) wsHandler(w http.ResponseWriter, r *http.Request) {
	if conn, _, _, err := ws.UpgradeHTTP(r, w); err != nil {
		return
	} else {
		if err := s.epoller.Add(conn); err != nil {
			log.Printf("Failed to add connection %v", err)
			conn.Close()
		}
	}
}

func (s *Server) listenForConnections() {
	log.Printf("Server started. Listening for websocket connections on port %s\n", s.port)
	for {
		if connections, err := s.epoller.Wait(); err != nil {
			continue
		} else {
			for _, conn := range connections {
				if conn == nil {
					break
				}
				if msg, _, err := wsutil.ReadClientData(conn); err != nil {
					if err := s.epoller.Remove(conn); err != nil {
						log.Printf("Failed to remove %v\n", err)
					}
					conn.Close()
				} else {
					// RECEIVE MESSAGE FROM CLIENT
					go s.HandleMessageFn(&conn, msg)
				}
			}
		}
	}
}

/*
Message Handling Stuff

*/
type rWriter struct {
	Found     bool   `json:"found"`
	Input     string `json:"userinput"`
	FinalHash string `json:"lasthash"`
}

func hashit(s *[]byte) {
	var out []byte
	dig := sha256.Sum256(*s)
	if len(*s) != 64 {
		out = make([]byte, 64)
		*s = out
	}
	hex.Encode(*s, dig[:])
}

func sendWebsocketMessage(c *net.Conn, msg *[]byte) {
	e := wsutil.WriteServerMessage(*c, ws.OpCode(1), *msg)
	if e != nil {
		log.Printf("Failed to send a message %v", e)
	}
}

func (s *Server) saveNewEntries(str string) error {
	path := s.saveLoc + "newhashes.csv"
	if file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		return err
	} else {
		defer file.Close()
		if _, err := file.WriteString(str); err != nil {
			return err
		}
		return nil
	}
}

func (s *Server) saveFoundEntries(str string) error {
        path := s.saveLoc + "found.csv"
        if file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
                return err
        } else {
                defer file.Close()
                if _, err := file.WriteString(str); err != nil {
                        return err
                }
                return nil
        }
}


func (s *Server) HandleMessageFn(c *net.Conn, incomming []byte) {
	var fv = string(incomming)
	var replyMsg []byte
	msg := incomming

//	log.Println("Message received:", string(incomming))
	_, ok := s.m.GET(string(msg))
	if ok {
		replyMsg, _ = json.Marshal(rWriter{Found: ok, Input: fv, FinalHash: string(msg)})
		sendWebsocketMessage(c, &replyMsg)
                s.saveFoundEntries(fmt.Sprintf("reqValue: %s,hashFound: %s, iValue: %d", fv,string(msg), -1))
		return
	}
	for i := 0; i < 1000000; i++ {
		hashit(&msg)
		_, ok := s.m.GET(string(msg))
		if ok {
			if (i == 999999){ break }
			replyMsg, _ = json.Marshal(rWriter{Found: ok, Input: fv, FinalHash: string(msg)})
			sendWebsocketMessage(c, &replyMsg)
			s.saveFoundEntries(fmt.Sprintf("reqValue: %s,hashFound: %s, iValue: %d", fv,string(msg), i))
			return
		}
	}

	go s.m.SET(string(msg), struct{}{})

	/* not Found */
	replyMsg, _ = json.Marshal(rWriter{Input: fv})
	sendWebsocketMessage(c, &replyMsg)

	s.saveNewEntries(fmt.Sprintf("%s,%s\n", fv, string(msg)))
	replyMsg = nil
	msg = nil
	incomming = nil
}
