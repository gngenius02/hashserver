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

func NewRedisClient() (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         ":6379",
		Password:     "",
		DB:           0,
		MaxRetries:   10,
		ReadTimeout:  time.Duration(30) * time.Second,
		WriteTimeout: time.Duration(30) * time.Second,
		PoolSize:     4,
	})

	return &Client{
		client: client,
	}, nil
}

func (c *Client) GetData(hArr *HashArray) (interface{}, error) {
	rdb, ha := c.client, *hArr

	dbResp, err := rdb.MGet(rdb.Context(), (ha)...).Result()
	if err != nil {
		return nil, err
	}

	firstItem := (ha)[0]
	for _, val := range dbResp {
		if val != nil && val != firstItem {
			return val, nil
		}
	}
	return nil, nil
}

func (c *Client) setData(d InsertData) error {
	rdb := c.client

	return rdb.Set(rdb.Context(), d.lastItem, d.firstItem, d.expiration).Err()

}

func (c *Client) CheckExist(hArr *HashArray) (int64, error) {
	rdb, ha := c.client, *hArr

	dbResp, err := rdb.Exists(rdb.Context(), ha...).Result()

	if err := c.setData(InsertData{ha[0], ha[len(ha)-1], time.Duration(0)}); err != nil {
		return 0, err
	}

	return dbResp, err

}
