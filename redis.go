package main

import (
	"time"

	"github.com/go-redis/redis/v8"
)

type Client struct {
	client *redis.Client
}

type InsertData struct {
	firstItem  string
	lastItem   string
	expiration time.Duration
}

type HashArray []string
type DbResponse []interface{}

func NewRedisClient() (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         ":6379",
		Password:     "",
		DB:           0,
		MaxRetries:   20,
		ReadTimeout:  time.Duration(1) * time.Minute,
		WriteTimeout: time.Duration(1) * time.Minute,
		PoolSize:     4,
	})
	return &Client{
		client: client,
	}, nil
}

func (d *DbResponse) checkResponseValues() interface{} {
	for _, val := range *d {
		if val != nil && val != (*d)[0] {
			return val
		}
	}
	return nil
}

func (c *Client) setData(d InsertData) error {
	rdb := c.client
	return rdb.Set(rdb.Context(), d.lastItem, d.firstItem, d.expiration).Err()
}

func (c *Client) GetData(h *HashArray) (interface{}, error) {
	var (
		response DbResponse
		err      error
		rdb      *redis.Client = c.client
	)
	response, err = rdb.MGet(rdb.Context(), (*h)...).Result()
	if err != nil {
		return nil, err
	}
	go c.setData(InsertData{(*h)[0], (*h)[len(*h)-1], time.Duration(0)})
	return response.checkResponseValues(), nil
}
