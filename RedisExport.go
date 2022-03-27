package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"os"
	"strconv"
	"strings"
	"time"
)

var RedisConn *redis.Pool
var (
	host = flag.String("h", "127.0.0.1", "redis host")
	port = flag.String("p", "6379", "port")

	username = flag.String("u", "", "username")
	password = flag.String("P", "", "password")

	dbNum   = flag.Int("n", 0, "db number")
	keyName = flag.String("k", "", "key name")
	scanKey = flag.String("s", "", "scan key")

	redisStringFormat = "SET %s %s"
	redisHashFormat   = "HMSET %s %s"
	redisListFormat   = "LPUSH %s %s"
	redisSetFormat    = "SADD %s %s"
	redisZSetFormat   = "ZADD %s %s"
)

func main() {
	fmt.Println("Export example: " + "go run .\\RedisExport.go -h 127.0.0.1 -p 6379 -k redisKey")
	fmt.Println("Import example: cat output-file | redis-cli -h 127.0.0.1 -p 6379")
	conn := RedisConn.Get()
	defer conn.Close()

	if len(*scanKey) == 0 && len(*keyName) == 0 {
		fmt.Println("Need key.")
		return
	}

	if len(*keyName) > 0 {
		// 单key处理
		oupPut, err := OneKeyDeal(*keyName)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(oupPut)
		}
	}

	if len(*scanKey) > 0 {
		offset := "0"
		allKeys := make([]string, 0)
		for {
			keys, nextOffset, err := ScanAllKeys(*scanKey, offset)
			if err != nil {
				fmt.Println("Scan error: " + *scanKey)
				return
			}

			allKeys = append(allKeys, keys...)
			offset = nextOffset
			if nextOffset == "0" {
				break
			}
		}

		if len(allKeys) == 0 {
			fmt.Println("do not match any keys")
			return
		}

		f, err := os.Create("./redis-output-" + time.Now().Format("20060102-150405"))
		if err != nil {
			fmt.Println("creat file error: " + err.Error())
			return
		}
		for _, eachKey := range allKeys {
			outPut, err := OneKeyDeal(eachKey)
			if err != nil {
				fmt.Println(err.Error())
			} else {
				_, err := f.WriteString(outPut + "\n")
				if err != nil {
					fmt.Println("write  " + outPut + " error.")
				}
			}
		}
	}
}

func OneKeyDeal(redisKey string) (string, error) {
	res := ""
	// 存在判断
	keyExists := Exists(redisKey)
	if keyExists == false {
		return res, errors.New("key not exists")
	}
	// Key 类型
	typeName, err := KeyType(redisKey)
	if err != nil {
		return res, err
	}

	switch typeName {
	case "string":
		val, err := Get(redisKey)
		if err != nil {
			return res, err
		}
		fmt.Println(redisKey+" string value:", string(val))
		res = fmt.Sprintf(redisStringFormat, redisKey, strconv.Quote(string(val)))
	case "hash":
		val, err := HGetAll(redisKey)
		if err != nil {
			return res, err
		}
		fmt.Println(redisKey+" hash map:", val)
		tmp := make([]string, 0)
		for k, v := range val {
			tmp = append(tmp, k)
			tmp = append(tmp, strconv.Quote(v))
		}
		allKV := strings.Join(tmp, " ")
		res = fmt.Sprintf(redisHashFormat, redisKey, allKV)
	case "list":
		val, err := GetAllList(redisKey)
		if err != nil {
			return res, err
		}
		fmt.Print(redisKey + " list values:")
		strSlice := make([]string, 0)
		for _, v := range val {
			fmt.Print(string(v), " ")
			strSlice = append(strSlice, strconv.Quote(string(v)))
		}
		fmt.Println()
		allElement := strings.Join(strSlice, " ")
		res = fmt.Sprintf(redisListFormat, redisKey, allElement)
	case "set":
		val, err := GetAllSet(redisKey)
		if err != nil {
			return res, err
		}
		fmt.Print(redisKey + " set values:")
		strSlice := make([]string, 0)
		for _, v := range val {
			fmt.Print(string(v), " ")
			strSlice = append(strSlice, strconv.Quote(string(v)))
		}
		fmt.Println()
		allElement := strings.Join(strSlice, " ")
		res = fmt.Sprintf(redisSetFormat, redisKey, allElement)
	case "zset":
		val, err := GetAllSortedSetMembers(redisKey)
		if err != nil {
			return res, err
		}
		fmt.Println(redisKey+" sorted set values:", val)
		strZSet := make([]string, 0)
		zSetLen := len(val)
		for i := zSetLen - 1; i >= 0; i-- {
			strZSet = append(strZSet, val[i])
		}
		allElement := strings.Join(strZSet, " ")
		res = fmt.Sprintf(redisZSetFormat, redisKey, allElement)
	default:
		return res, errors.New("key type not support")
	}
	return res, nil
}

func init() {
	flag.Parse()
	RedisConn = &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", *host+":"+*port, redis.DialDatabase(*dbNum))
			if err != nil {
				return nil, err
			}
			if *password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					_ = c.Close()
					return nil, err
				}
			}

			return c, err
		},

		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
func Exists(key string) bool {
	conn := RedisConn.Get()
	defer conn.Close()

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return false
	}

	return exists
}

func KeyType(key string) (string, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	typeName, err := redis.String(conn.Do("TYPE", key))
	return typeName, err
}

func Get(key string) ([]byte, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func HGetAll(key string) (map[string]string, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.StringMap(conn.Do("HGETALL", key))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func GetAllList(key string) ([][]byte, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.ByteSlices(conn.Do("LRANGE", key, 0, -1))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func GetAllSet(key string) ([][]byte, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.ByteSlices(conn.Do("SMEMBERS", key))
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func GetAllSortedSetMembers(key string) ([]string, error) {
	conn := RedisConn.Get()
	defer conn.Close()

	reply, err := redis.Strings(conn.Do("ZRANGE", key, 0, -1, "WITHSCORES"))
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func ScanAllKeys(scanKey string, offset string) ([]string, string, error) {
	conn := RedisConn.Get()
	defer conn.Close()
	res := make([]string, 0)
	nextOffsetStr := ""

	reply, err := redis.Values(conn.Do("SCAN", offset, "MATCH", scanKey, "COUNT", 100))
	if err != nil {
		return res, nextOffsetStr, err
	}
	if len(reply) != 2 {
		return res, nextOffsetStr, errors.New("scan result error")
	}

	if nextOffset, ok := reply[0].([]byte); ok {
		nextOffsetStr = string(nextOffset)
	} else {
		return res, nextOffsetStr, errors.New("scan result[0] error")
	}

	if allKeys, ok := reply[1].([]interface{}); ok {
		for i, v := range allKeys {
			if v2, ok := v.([]byte); ok {
				res = append(res, string(v2))
			} else {
				return res, nextOffsetStr, errors.New("scan result[1][" + strconv.FormatInt(int64(i), 10) + "] error")
			}
		}
	} else {
		return res, nextOffsetStr, errors.New("scan result[1] error")
	}

	return res, nextOffsetStr, nil
}
