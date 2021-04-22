package main

import (
	"crypto/md5"
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

func (h *HashArray) hashThem() {
	gets256 := func(s string) string {
		dig := sha256.Sum256([]byte(s))
		return hex.EncodeToString(dig[:])
	}
	getmd5 := func(s string) string {
		dig := md5.Sum([]byte(s))
		return hex.EncodeToString(dig[:])
	}
	ha := *h
	for i := 1; i < len(ha); i++ {
		ha[i] = gets256(ha[i-1])
	}
	for i := 1; i < len(ha); i++ {
		ha[i] = getmd5(ha[i])
	}
}

func (h *HashArray) getLastItem() string {
	return (*h)[len(*h)-1]
}

func checkError(e error) {
	if e != nil {
		log.Fatal(e)
		// log.Println("error checking if entry exists in redis or writing to db.", err)
	}
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
	app.Get("api/getdbsize", func(c *fiber.Ctx) error {
		dbsize, e := redis.client.DBSize(redis.client.Context()).Result()
		if e != nil {
			return c.Next()
		}
		return c.JSON(dbsize * 1000000)
	})
	app.Get("api/million/:id", func(c *fiber.Ctx) error {
		length := 1000000 + 1
		firstValue := c.Params("id")

		hashArr := make(HashArray, length)
		hashArr[0] = firstValue
		hashArr.hashThem()

		lastValue := hashArr.getLastItem()
		exists, err := redis.CheckExist(&hashArr)
		checkError(err)

		if exists > 0 {
			foundVal, err := redis.GetData(&hashArr)
			checkError(err)

			if foundVal != nil {
				go Write2File(WriteFileStruct{fmt.Sprintf("seed: %v, hash: %v, lastItem: %v", foundVal, firstValue, lastValue), "/home/node/foundhashes.csv"})
				return c.JSON(&response{true, foundVal, firstValue})
			}
		}
		go Write2File(WriteFileStruct{firstValue + "," + lastValue, "/home/node/newhashes.csv"})
		return c.JSON(&response{false, "", firstValue})
	})
	app.Use(func(c *fiber.Ctx) error {
		return c.SendStatus(404)
	})
	log.Fatal(app.Listen(":3000"))
}
