package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

var rdb *redis.Client = redis.NewClient(&redis.Options{
	Addr:         ":6379",
	Password:     "",
	DB:           0,
	MaxRetries:   10,
	ReadTimeout:  time.Duration(30) * time.Second,
	WriteTimeout: time.Duration(30) * time.Second,
	PoolSize:     4,
})

type response struct {
	Found bool        `json:"found"`
	Seed  interface{} `json:"seed"`
	Hash  string      `json:"hash"`
}

type writeFileStruct struct {
	Data  string
	Where string
}

type hashArray []string

func toHex(s [32]byte) string {
	return hex.EncodeToString(s[:])
}

func checkExist(ha *hashArray, c *fiber.Ctx) error {
	firstItem := (*ha)[0]
	lastItem := (*ha)[len(*ha)-1]
	dbResp, _ := rdb.Exists(rdb.Context(), (*ha)...).Result()
	if dbResp > 0 {
		dbResp, _ := rdb.MGet(rdb.Context(), (*ha)...).Result()
		for _, val := range dbResp {
			if val != nil && val != firstItem {
				go write2File(writeFileStruct{fmt.Sprintf("seed: %v, hash: %v, lastItem: %v", val, firstItem, lastItem), "/home/node/foundhashes.csv"})
				return c.JSON(&response{true, val, firstItem})
			}
		}
	}
	if err := rdb.Set(rdb.Context(), lastItem, firstItem, 0).Err(); err != nil {
		panic(err)
	}
	go write2File(writeFileStruct{firstItem + "," + lastItem, "/home/node/newhashes.csv"})
	return c.JSON(&response{false, "", firstItem})
}

func generateHashValues(hashArr *hashArray) {
	var hash string
	for i := 1; i < len(*hashArr); i++ {
		hash = toHex(sha256.Sum256([]byte((*hashArr)[i-1])))
		(*hashArr)[i] = hash
	}
}

func checkExistHandler(c *fiber.Ctx) error {
	const length int = 1000000 + 1

	hash := c.Params("id")
	hashArr := make(hashArray, length)
	hashArr[0] = hash

	generateHashValues(&hashArr)
	return checkExist(&hashArr, c)
}

func write2File(data writeFileStruct) {
	var (
		f   *os.File
		err error
	)
	if f, err = os.OpenFile(data.Where, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		log.Println(err)
	}
	defer f.Close()
	if _, err := f.WriteString(data.Data + "\n"); err != nil {
		log.Println(err)
	}
}

func main() {
	defer rdb.Close()
	runtime.GOMAXPROCS(runtime.NumCPU() / 4)

	app := fiber.New(fiber.Config{
		Prefork: true,
	})
	app.Use(cors.New())

	app.Get("api/million/:id", checkExistHandler)

	// => 404 "Not Found"
	app.Use(func(c *fiber.Ctx) error {
		return c.SendStatus(404)
	})
	log.Fatal(app.Listen(":3000"))
}
