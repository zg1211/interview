package meta

import (
	"errors"
	"golang.org/x/time/rate"
	"math/rand"
	"time"
)

var (
	ErrBadNetwork        = errors.New("bad network")
	ErrRateLimited       = errors.New("rate limited")
	ErrInvalidADID       = errors.New("invalid ad id")
	ErrInvalidCreativeID = errors.New("invalid creative id")
)

type Client struct {
	limiter *rate.Limiter
}

func NewClient() *Client {
	lim := rate.NewLimiter(10, 10)
	time.Sleep(1 * time.Second)
	return &Client{
		limiter: lim,
	}
}

func (c *Client) UpdateADCreative(adID, creativeID string) error {
	// 5 percent bad network rate
	if rand.Int31n(100) < 10 {
		return ErrBadNetwork
	}
	// network transfer time
	time.Sleep(time.Millisecond * time.Duration(300+rand.Int31n(200)))
	// rate limit
	if !c.limiter.Allow() {
		return ErrRateLimited
	}
	// 1 percent invalid ad id
	if rand.Int31n(100) < 1 {
		return ErrInvalidADID
	}
	// 1 percent invalid creative id
	if rand.Int31n(100) < 1 {
		return ErrInvalidCreativeID
	}
	// processing time
	time.Sleep(time.Millisecond * time.Duration(500+rand.Int31n(1000)))
	return nil
}
