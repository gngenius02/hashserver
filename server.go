package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"runtime"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

type response struct {
	Found bool        `json:"found"`
	Seed  interface{} `json:"seed"`
	Hash  string      `json:"hash"`
}

func toHex(s [32]byte) string {
	return hex.EncodeToString(s[:])
}

func (hashArr *HashArray) generateHashValues() {
	var hash string
	for i := 1; i < len(*hashArr); i++ {
		hash = toHex(sha256.Sum256([]byte((*hashArr)[i-1])))
		(*hashArr)[i] = hash
	}
}
func (hashArr *HashArray) getLastItem() string {
	return (*hashArr)[len(*hashArr)-1]
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() / 4)

	var (
		redis *Client
		rErr  error
	)

	if redis, rErr = NewRedisClient(); rErr != nil {
		log.Fatal("Couldnt connect to redis instance", rErr)
	}

	app := fiber.New(fiber.Config{
		Prefork: true,
	})
	app.Use(cors.New())
	app.Get("api/million/:id", func(c *fiber.Ctx) error {
		length := 1000000 + 1
		hash := c.Params("id")

		hashArr := make(HashArray, length)
		hashArr[0] = hash
		hashArr.generateHashValues()

		lastItem := hashArr.getLastItem()
		resp, err := redis.CheckExist(&hashArr)

		if err != nil {
			log.Println("error checking if entry exists in redis or writing to db.", err)
			return c.JSON(err)
		}

		if resp > 0 {
			val, err := redis.GetData(&hashArr)
			if err != nil {
				return c.Next()
			}
			go Write2File(WriteFileStruct{fmt.Sprintf("seed: %v, hash: %v, lastItem: %v", val, hash, lastItem), "/home/node/foundhashes.csv"})
			return c.JSON(&response{true, val, hash})
		}
		go Write2File(WriteFileStruct{hash + "," + lastItem, "/home/node/newhashes.csv"})
		return c.JSON(&response{false, "", hash})
	})
	app.Use(func(c *fiber.Ctx) error {
		return c.SendStatus(404)
	})
	log.Fatal(app.Listen(":4444"))
}
