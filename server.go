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

func (h *HashArray) getHashes() {
	gets256 := func(s string) string {
		dig := sha256.Sum256([]byte(s))
		return hex.EncodeToString(dig[:])
	}
	ha := *h
	for i := 1; i < len(ha); i++ {
		ha[i] = gets256(ha[i-1])
	}
}

func (h *HashArray) getLastItem() string {
	return (*h)[len(*h)-1]
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() / 4)

	var (
		redis *Client
		err   error
	)

	if redis, err = NewRedisClient(); err != nil {
		log.Fatal("Couldnt connect to redis instance", err)
	}

	app := fiber.New(fiber.Config{
		Prefork: true,
	})
	app.Use(cors.New())
	app.Get("api/getdbsize", func(c *fiber.Ctx) error {
		dbsize, err := redis.client.DBSize(redis.client.Context()).Result()
		if err != nil {
			return c.SendStatus(404)
		}
		return c.JSON(dbsize * 1000000)
	})

	app.Get("api/million/:id", func(c *fiber.Ctx) error {
		length := 1000000 + 1
		firstValue := c.Params("id")

		h := make(HashArray, length)
		h[0] = firstValue
		h.getHashes()

		lastValue := h.getLastItem()
		foundVal, err := redis.GetData(&h)
		if err != nil {
			return c.SendStatus(404)
		}

		if foundVal != nil {
			go Write2File(WriteFileStruct{fmt.Sprintf("seed: %v, hash: %v, lastItem: %v", foundVal, firstValue, lastValue), "/home/node/foundhashes.csv"})
			return c.JSON(&response{true, foundVal, firstValue})
		}

		go Write2File(WriteFileStruct{firstValue + "," + lastValue, "/home/node/newhashes.csv"})
		return c.JSON(&response{false, "", firstValue})
	})
	app.Use(func(c *fiber.Ctx) error {
		return c.SendStatus(404)
	})
	log.Fatal(app.Listen(":3000"))
}
