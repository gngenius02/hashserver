package redis

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"startgoserver/common"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

var rdb *redis.Client = redis.NewClient(&redis.Options{
	Addr:         "/var/run/redis/redis-server.sock",
	Password:     "",
	DB:           0,
	MaxRetries:   20,
	ReadTimeout:  time.Duration(1) * time.Minute,
	WriteTimeout: time.Duration(1) * time.Minute,
})

type insertData struct {
	FirstItem  string
	LastItem   string
	Expiration time.Duration
}

type rWriter struct {
	Found bool        `json:"found"`
	Seed  interface{} `json:"seed"`
	Hash  string      `json:"hash"`
}

type DbResponse []interface{}

func addNewEntry(d insertData) error {
	return rdb.Set(rdb.Context(), d.LastItem, d.FirstItem, d.Expiration).Err()
}

func (d DbResponse) CheckResponseValues() interface{} {
	for _, val := range d {
		if val != nil && val != (d)[0] {
			return val
		}
	}
	return nil
}

func b2h(s string) string {
	if len(s) == 43 {
		b, _ := base64.RawStdEncoding.DecodeString(s)
		return hex.EncodeToString(b)
	}
	return s
}

func GetData(h common.HashArray, conn net.Conn) {
	var err error
	var dbresp DbResponse

	if dbresp, err = rdb.MGet(rdb.Context(), h...).Result(); err != nil {
		log.Printf("MY Error: Database MGET Failed: %v", err)
	} else {
		firstHash, lastHash := h[0], h[len(h)-1]

		// checks db for data && checks to see if matched seed != firsthash (false positive)
		if foundValue := dbresp.CheckResponseValues(); foundValue != nil && foundValue != firstHash { // Found in DB
			data, _ := json.Marshal(&rWriter{true, foundValue, b2h(firstHash)})
			go sendResponseToClient(conn, data)
		} else { // Not Found in DB
			go addNewEntry(insertData{firstHash, lastHash, time.Duration(0)})
			data, _ := json.Marshal(&rWriter{false, "", b2h(firstHash)})
			go sendResponseToClient(conn, data)
		}
	}
}

func sendResponseToClient(conn net.Conn, msg []byte) {
	if err := wsutil.WriteServerMessage(conn, ws.OpText, msg); err != nil {
		log.Printf("Failed to send a emssage %v", err)
	}
}
