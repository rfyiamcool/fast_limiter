package fastLimiter

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

func initRedis(opt Options) *redis.Pool {
	redisClient := &redis.Pool{
		MaxIdle:   5,
		MaxActive: 5,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", opt.AddrPort, redis.DialPassword(opt.Password), redis.DialDatabase(opt.DB))
			if err != nil {
				logger(err.Error())
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			if err != nil {
				logger(err.Error())
			}
			return err
		},
	}

	return redisClient
}
