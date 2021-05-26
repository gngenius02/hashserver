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

func (s *Server) HandleMessageFn(c *net.Conn, incomming []byte) {
	var fv = string(incomming)
	var replyMsg []byte
	msg := incomming

	fmt.Println("Message received:", string(incomming))
	_, ok := s.m.GET(string(msg))
	if ok {
		replyMsg, _ = json.Marshal(rWriter{Found: ok, Input: fv, FinalHash: string(msg)})
		sendWebsocketMessage(c, &replyMsg)
		return
	}
	for i := 0; i < 1000000; i++ {
		hashit(&msg)
		_, ok := s.m.GET(string(msg))
		if ok {
			replyMsg, _ = json.Marshal(rWriter{Found: ok, Input: fv, FinalHash: string(msg)})
			sendWebsocketMessage(c, &replyMsg)
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
	//	runtime.GC()
}

//
//
//
//
//
//
//
// N, B := 1024, 32

// ploadTimer := time.Now()
// pkeys := pRandomData(N, B)
// fmt.Println("loaded", len(pkeys), "items.", pkeys[len(pkeys)-1], "It took", time.Since(ploadTimer))

// // loadTimer := time.Now()
// // keys := randomData(N, B)
// // fmt.Println("loaded", len(keys), "items.", keys[len(keys)-1], "It took", time.Since(loadTimer))

// checkKeys := func(keys ...string) {
// 	for i, v := range keys {
// 		if len(v) != 43 {
// 			fmt.Println(i, v, keys[i])
// 		}
// 	}
// }
// checkKeys(keys...)
// checkKeys(pkeys...)

// loadTimer := time.Now()
// pkeys := PLoadDB(N)
// fmt.Println("loaded", N, "items into Parallel DB. It took", time.Since(loadTimer))

// loadTimer = time.Now()
// keys := LoadDB(N)
// fmt.Println("loaded", N, "items into Regular DB. It took", time.Since(loadTimer))

// start := time.Now()
// pmv, pmo := PMGET(pkeys...)
// pmgetTime := time.Since(start)

// start = time.Now()
// mv, mo := MGET(keys...)
// mgetTime := time.Since(start)

// dbsize := m.DBSIZE()

// fmt.Println("Actual number in DB:", dbsize, "number in keys array", len(keys), "number in pkeys array", len(pkeys))

// fmt.Printf("\nfinished checking all Functions:\n")
// fmt.Println("MGET checked all keys in (ms)", mgetTime.Milliseconds(), mv, mo)
// fmt.Println("PMGET checked all keys in (ms)", pmgetTime.Milliseconds(), pmv, pmo)

// for i := 0; i < 10; i++ {
// 	wg.Add(1)
// 	go func(i int) {
// 		defer wg.Done()
// 		for j := 0; j < 10; j++ {
// 			// log.Println(i, j)
// 		}
// 	}(i)
// }
// wg.Wait()

// var shards int = 1
// for shards < 48*16 {
// 	log.Println(shards)
// 	shards *= 2
// }

// log.Println("runtime cpu: ", runtime.NumCPU(), 48*16)
// log.Println("shards number is: ", shards)

// package main

// import (
// 	"crypto/sha256"
// 	"encoding/hex"
// 	"fmt"
// 	"log"
// 	"runtime"

// 	"github.com/gofiber/fiber/v2"
// 	"github.com/gofiber/fiber/v2/middleware/cors"
// )

// type response struct {
// 	Found bool        `json:"found"`
// 	Seed  interface{} `json:"seed"`
// 	Hash  string      `json:"hash"`
// }

// func (h *HashArray) getHashes() {
// 	gets256 := func(s string) string {
// 		dig := sha256.Sum256([]byte(s))
// 		return hex.EncodeToString(dig[:])
// 	}
// 	ha := *h
// 	for i := 1; i < len(ha); i++ {
// 		ha[i] = gets256(ha[i-1])
// 	}
// }

// func (h *HashArray) getLastItem() string {
// 	return (*h)[len(*h)-1]
// }

// func main() {
// 	runtime.GOMAXPROCS(runtime.NumCPU() / 4)

// 	var (
// 		clients       [8]Client
// 		foundFile     *Fs
// 		newHashesFile *Fs
// 		err           error
// 		foundPath     string = "/home/node/found.csv"
// 		newPath       string = "/home/node/newhashes.csv"
// 	)

// 	for i := range clients {
// 		var redis *Client
// 		if redis, err = NewRedisClient(); err != nil {
// 			log.Fatal("Couldnt connect to redis instance", err)
// 		}
// 		clients[i] = *redis
// 		defer clients[i].client.Close()
// 	}

// 	if foundFile, err = FileOpen(foundPath); err != nil {
// 		log.Fatal("Couldnt open file", foundPath, err)
// 	}
// 	defer foundFile.CloseFile()

// 	if newHashesFile, err = FileOpen(newPath); err != nil {
// 		log.Fatal("Couldnt open file", foundPath, err)
// 	}
// 	defer newHashesFile.CloseFile()

// 	counter := 0

// 	pickClient := func() *Client {
// 		counter = counter + 1
// 		return &clients[counter%8]
// 	}

// 	app := fiber.New(fiber.Config{
// 		Prefork: true,
// 	})
// 	app.Use(cors.New())

// 	app.Get("api/getdbsize", func(c *fiber.Ctx) error {
// 		redis := pickClient()
// 		dbsize, err := redis.client.DBSize(redis.client.Context()).Result()
// 		if err != nil {
// 			return c.Next()
// 		}
// 		return c.JSON(dbsize * 1000000)
// 	})

// 	app.Get("api/million/:id", func(c *fiber.Ctx) error {
// 		length := 1000000 + 1
// 		firstValue := c.Params("id")

// 		h := make(HashArray, length)
// 		h[0] = firstValue
// 		h.getHashes()

// 		lastValue := h.getLastItem()
// 		redis := pickClient()
// 		foundVal, err := redis.GetData(&h)
// 		if err != nil {
// 			return c.Next()
// 		}

// 		if foundVal != nil && foundVal != firstValue {
// 			go foundFile.Write2File(fmt.Sprintf("seed: %v, hash: %v, lastItem: %v", foundVal, firstValue, lastValue))
// 			return c.JSON(&response{true, foundVal, firstValue})
// 		}

// 		go newHashesFile.Write2File(firstValue + "," + lastValue)
// 		return c.JSON(&response{false, "", firstValue})
// 	})
// 	app.Use(func(c *fiber.Ctx) error {
// 		return c.SendStatus(404)
// 	})
// 	log.Fatal(app.Listen(":3000"))
// }
// func PMGET(keys ...string) (value interface{}, ok bool) {
// 	splitby, part := runtime.NumCPU()*16, len(keys)/(runtime.NumCPU()*16)
// 	if len(keys)%splitby != 0 {
// 		splitby = splitby + 1
// 	}
// 	for i := 0; i < splitby; i++ {
// 		wg.Add(1)
// 		start, end := part*i, part*(i+1)
// 		if end > len(keys) {
// 			end = len(keys)
// 		}
// 		go func(portionOfKeys ...string) {
// 			defer wg.Done()
// 			for _, key := range portionOfKeys {
// 				if v, o := m.GET(key); v != nil || o {
// 					value, ok = v, true
// 					break
// 				}
// 			}
// 		}(keys[start:end]...)
// 	}
// 	wg.Wait()
// 	return value, ok
// }

// func MGET(keys ...string) (value interface{}, ok bool) {
// 	for _, key := range keys {
// 		if v, o := m.GET(key); v != nil || o {
// 			return v, o
// 		}
// 	}
// 	return value, ok
// }

// func getRandom(bytes int) string {
// 	randBytes := make([]byte, bytes)
// 	rand.Read(randBytes)
// 	return base64.RawURLEncoding.Strict().EncodeToString(randBytes)
// }

// func (m *Map) PMSET(keys ...[]string) (value interface{}, ok bool) {
// 	var wg sync.WaitGroup
// 	blocks, blockSize := runtime.NumCPU()*16, len(keys)/(runtime.NumCPU()*16)

// 	if len(keys)%blocks != 0 {
// 		blocks = blocks + 1
// 	}
// 	for i := 0; i < blocks; i++ {
// 		wg.Add(1)
// 		start, end := blockSize*i, blockSize*(i+1)
// 		if end > len(keys) {
// 			end = len(keys)
// 		}
// 		go func(block ...[]string) {
// 			defer wg.Done()
// 			for _, pair := range block {
// 				key, value := pair[0], pair[1]
// 				m.SET(key, value)
// 			}
// 		}(keys[start:end]...)
// 	}
// 	wg.Wait()
// 	return value, ok
// }

// func MSET(keys ...[]string) {
// 	for _, pair := range keys {
// 		key, value := pair[0], pair[1]
// 		m.SET(key, value)
// 	}
// }

// func PLoadDB(N int) (pkeys []string) {
// 	Bytes, splitby, part := 32, runtime.NumCPU(), N/runtime.NumCPU()
// 	if N%runtime.NumCPU() != 0 {
// 		splitby = splitby + 1
// 	}
// 	pkeys = make([]string, N)
// 	for i := 0; i < splitby; i++ {
// 		wg.Add(1)
// 		start, end := part*i, part*(i+1)
// 		if end > N {
// 			end = N
// 		}
// 		go func(s, e int) {
// 			defer wg.Done()
// 			for i := s; i < e; i++ {
// 				randkey := getRandom(Bytes)
// 				m.SET(randkey, getRandom(Bytes))
// 				if i == N-1 {
// 					pkeys[i] = randkey
// 					break
// 				}
// 				pkeys[i] = getRandom(Bytes)
// 			}
// 		}(start, end)
// 	}
// 	wg.Wait()
// 	return pkeys
// }

// func LoadDB(N int) (keys []string) {
// 	Bytes := 32
// 	keys = make([]string, 0)
// 	for i := 0; i < N; i++ {
// 		randkey := getRandom(Bytes)
// 		m.SET(randkey, getRandom(Bytes))
// 		if i != N-1 {
// 			keys = append(keys, getRandom(Bytes))
// 			continue
// 		}
// 		keys = append(keys, randkey)
// 	}
// 	return keys
// }
