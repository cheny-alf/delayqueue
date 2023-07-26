package delayqueue

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"log"
	"math"
	"time"
)

type DelayQueue struct {
	name          string            //队列名称，保证当前队列在redis中是唯一的
	redisCli      *redis.Client     //redis 客户端
	cb            func(string) bool //回调函数
	pendingKey    string            //sortedset 存储未到投递时间的消息 member为消息ID，score为投递时间
	readyKey      string            //list 存储已经到投递时间的消息 element为消息ID
	unAckKey      string            //sortedset 存储已经投递，但为确认的消息 member为消息ID，score为处理超时时间，超出时间还没ack的消息会被重试
	retryKey      string            //list 存储超时后待重试的消息 element为消息ID
	retryCountKey string            //hash 存储重试次数 field为消息ID，value为重试次数
	garbageKey    string            //set 暂时存储已达重试上限的消息 member为消息ID
	ticker        *time.Ticker
	logger        *log.Logger
	close         chan struct{}

	maxConsumeDuration time.Duration
	msgTTL             time.Duration
	defaultRetryCount  uint
	fetchInterval      time.Duration
	fetchLimit         uint
}

// NewDelayQueue 创建新的Queue
func NewDelayQueue(name string, redisCli *redis.Client, callback func(string) bool) *DelayQueue {
	if name == "" {
		panic("name is required")
	}
	if redisCli == nil {
		panic("redis client is required")
	}
	if callback == nil {
		panic("callback is required")
	}
	return &DelayQueue{
		name:               name,
		redisCli:           redisCli,
		cb:                 callback,
		pendingKey:         "dp:" + name + ":pending",
		readyKey:           "dp:" + name + ":ready",
		unAckKey:           "dp:" + name + ":unack",
		retryKey:           "dp:" + name + ":retry",
		retryCountKey:      "dp:" + name + ":retry:cnt",
		garbageKey:         "dp:" + name + ":garbage",
		logger:             log.Default(),
		close:              make(chan struct{}, 1),
		maxConsumeDuration: 5 * time.Second,
		msgTTL:             time.Hour,
		defaultRetryCount:  3,
		fetchInterval:      time.Second,
		fetchLimit:         math.MaxInt32,
	}
}

// WithLogger 自定义日志
func (q *DelayQueue) WithLogger(logger *log.Logger) *DelayQueue {
	q.logger = logger
	return q
}

// WithFetchInterval 配置从redis中拉取消息时间间隔
func (q *DelayQueue) WithFetchInterval(d time.Duration) *DelayQueue {
	q.fetchInterval = d
	return q
}

// WithMaxConsumeDuration 配置消息的超时时间
// 如果在消息传递后WithMaxConsumeDuration内未收到确认，DelayQueue将尝试再次传递此消息
func (q *DelayQueue) WithMaxConsumeDuration(d time.Duration) *DelayQueue {
	q.maxConsumeDuration = d
	return q
}

// WithFetchLimit 配置单次拉取消息的数量
func (q *DelayQueue) WithFetchLimit(limit uint) *DelayQueue {
	q.fetchLimit = limit
	return q
}

// WithDefaultRetryCount 自定义最大重试次数
func (q *DelayQueue) WithDefaultRetryCount(count uint) *DelayQueue {
	q.defaultRetryCount = count
	return q
}

func (q *DelayQueue) genMsgKey(idStr string) string {
	return "dp:" + q.name + ":msg:" + idStr
}

type retryCountOpt int

// WithRetryCount 给消息设置最大重试次数
// example: queue.SendDelayMsg(payload, duration, delayqueue.WithRetryCount(3))
func WithRetryCount(count int) interface{} {
	return retryCountOpt(count)
}

// SendScheduleMsg 发送定时消息
func (q *DelayQueue) SendScheduleMsg(payload string, t time.Time, opts ...interface{}) error {
	// parse options
	retryCount := q.defaultRetryCount
	for _, opt := range opts {
		switch o := opt.(type) {
		case retryCountOpt:
			retryCount = uint(o)
		}
	}
	idStr := uuid.Must(uuid.NewRandom()).String()
	ctx := context.Background()
	now := time.Now()

	//存储消息
	msgTTL := t.Sub(now) + q.msgTTL
	err := q.redisCli.Set(ctx, q.genMsgKey(idStr), payload, msgTTL).Err()
	if err != nil {
		return fmt.Errorf("store msg failed: %v", err)
	}
	//记录重试次数
	err = q.redisCli.HSet(ctx, q.retryCountKey, idStr, retryCount).Err()
	if err != nil {
		return fmt.Errorf("store retry count failed: %v", err)
	}
	//加入pending队列
	err = q.redisCli.ZAdd(ctx, q.pendingKey, &redis.Z{Score: float64(t.Unix()), Member: idStr}).Err()
	if err != nil {
		return fmt.Errorf("push to pending failed: %v", err)
	}
	return nil
}

// SendDelayMsg 发送延时消息
func (q *DelayQueue) SendDelayMsg(payload string, duration time.Duration, opts ...interface{}) error {
	t := time.Now().Add(duration)
	return q.SendScheduleMsg(payload, t, opts...)
}

// pending2ReadyScript 将消息从pending列表移入ready列表 保证原子性
// 参数：currentTime、pendingKey、readyKey
const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', KEYS[2], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end
local args2 = {'LPush', KEYS[3]} -- push into ready
for _,v in ipairs(msgs) do
		table.insert(args2,v)
end
redis.call(unpack(args2))
redis.call('ZRemRangeByScore',KEYS[1],'0',ARGV[1])
`

func (q *DelayQueue) pending2Ready() error {
	now := time.Now().Unix()
	ctx := context.Background()
	keys := []string{q.pendingKey, q.readyKey}
	err := q.redisCli.Eval(ctx, pending2ReadyScript, keys, now).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("pending2ReadyScript failed: %v", err)
	}
	return nil
}

// ready2UnackScript 将一条等待投递的消息从 ready （或 retry） 移动到 unack 中，并把消息发送给消费者。
// 参数: retryTime, readyKey/retryKey, unackKey
const ready2UnackScript = `
local msg = redis.call('RPop',KEYS[1])
if (not msg) then return end
redis.call('ZAdd',KEYS[2],ARGV[1],msg)
return msg
`

func (q *DelayQueue) ready2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	ctx := context.Background()
	keys := []string{q.readyKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ctx, ready2UnackScript, keys, retryTime).Result()
	if err == redis.Nil {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

func (q *DelayQueue) retry2Unack() (string, error) {
	retryTime := time.Now().Add(q.maxConsumeDuration).Unix()
	ctx := context.Background()
	keys := []string{q.retryKey, q.unAckKey}
	ret, err := q.redisCli.Eval(ctx, ready2UnackScript, keys, retryTime, q.retryKey, q.unAckKey).Result()
	if err == redis.Nil {
		return "", redis.Nil
	}
	if err != nil {
		return "", fmt.Errorf("ready2UnackScript failed %v", err)
	}
	str, ok := ret.(string)
	if !ok {
		return "", fmt.Errorf("illegal result: %#v", ret)
	}
	return str, nil
}

func (q *DelayQueue) callback(idStr string) (bool, error) {
	ctx := context.Background()
	payload, err := q.redisCli.Get(ctx, q.genMsgKey(idStr)).Result()
	if err == redis.Nil {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("get message payload failed:%v", err)
	}
	return q.cb(payload), nil
}

func (q *DelayQueue) ack(idStr string) error {
	ctx := context.Background()
	err := q.redisCli.ZRem(ctx, q.unAckKey, idStr).Err()
	if err != nil {
		return fmt.Errorf("remove from unack failed: %v", err)
	}
	// msg key has ttl, ignore result of delete
	_ = q.redisCli.Del(ctx, q.genMsgKey(idStr)).Err()
	q.redisCli.HDel(ctx, q.retryCountKey, idStr)
	return nil
}

func (q DelayQueue) nack(idStr string) error {
	ctx := context.Background()
	//更新重试时间为现在，unack2Retry 将立即将其重试
	err := q.redisCli.ZAdd(ctx, q.unAckKey, &redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: idStr,
	}).Err()
	if err != nil {
		return fmt.Errorf("negative ack failed:%v", err)
	}
	return nil
}

// unack2RetryScript 将retryCount>0的消息从unack列表 移动到retry列表中
// 由于DelayQueue无法在eval unack2RetryScript之前确定垃圾消息，
// 因此无法将keys参数传递给redisCli.eval
// 因此unack2ReteryScript将垃圾消息移动到garbageKey，而不是直接删除
// KEYS: currentTime, unackKey, retryCountKey, retryKey, garbageKey
const unack2RetryScript = `
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
for i,v in ipairs(retryCounts) do
	local k = msgs[i]
	if tonumber(v) > 0 then
		redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
		redis.call("LPush", KEYS[3], k) -- add to retry
	else
		redis.call("HDel", KEYS[2], k) -- del retry count
		redis.call("SAdd", KEYS[4], k) -- add to garbage
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
`

func (q *DelayQueue) unack2Retry() error {
	ctx := context.Background()
	keys := []string{q.unAckKey, q.retryCountKey, q.retryKey, q.garbageKey}
	now := time.Now()
	err := q.redisCli.Eval(ctx, unack2RetryScript, keys, now.Unix()).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("unack to retry script failed:%v", err)
	}
	return nil
}

// garbageCollect 清理已到最大重试次数的消息
func (q *DelayQueue) garbageCollect() error {
	ctx := context.Background()
	msgIds, err := q.redisCli.SMembers(ctx, q.garbageKey).Result()
	if err != nil {
		return fmt.Errorf("smembers failed:%v", err)
	}
	if len(msgIds) == 0 {
		return nil
	}
	// allow concurrent clean
	msgKeys := make([]string, 0, len(msgIds))
	for _, idStr := range msgIds {
		msgKeys = append(msgKeys, q.genMsgKey(idStr))
	}
	err = q.redisCli.Del(ctx, msgKeys...).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("del msgs failed: %v", err)
	}
	err = q.redisCli.SRem(ctx, q.garbageKey, msgIds).Err()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("remove from garbage key failed:%v", err)
	}
	return nil
}

// consume 消费消息
func (q *DelayQueue) consume() error {
	//pending2Ready
	err := q.pending2Ready()
	if err != nil {
		return err
	}
	//consume
	var fetchCount uint
	for true {
		idStr, err := q.ready2Unack()
		if err == redis.Nil {
			break
		}
		if err != nil {
			return err
		}
		fetchCount++
		ack, err := q.callback(idStr)
		if err != nil {
			return err
		}
		if ack {
			err = q.ack(idStr)
		} else {
			err = q.nack(idStr)
		}
		if err != nil {
			return err
		}
		if fetchCount >= q.fetchLimit {
			break
		}
	}
	// unack to retry
	err = q.unack2Retry()
	if err != nil {
		return err
	}
	err = q.garbageCollect()
	if err != nil {
		return err
	}
	//retry
	fetchCount = 0
	for true {
		idStr, err := q.retry2Unack()
		if err == redis.Nil {
			break
		}
		if err != nil {
			return err
		}
		fetchCount++
		ack, err := q.callback(idStr)
		if err != nil {
			return err
		}
		if ack {
			err = q.ack(idStr)
		} else {
			err = q.nack(idStr)
		}
		if err != nil {
			return err
		}
		if fetchCount >= q.fetchLimit {
			break
		}
	}
	return nil
}

// StartConsume 创建一个协程去队列中消费消息
// 使用 `<-done`来让消费者等待
func (q *DelayQueue) StartConsume() (done <-chan struct{}) {
	q.ticker = time.NewTicker(q.fetchInterval)
	done0 := make(chan struct{})
	go func() {
	tickerLoop:
		for true {
			select {
			case <-q.ticker.C:
				err := q.consume()
				if err != nil {
					log.Printf("consume error: %v", err)
				}
			case <-q.close:
				break tickerLoop
			}
		}
		done0 <- struct{}{}
	}()
	return done0
}

// StopConsume 停止消费者协程
func (q *DelayQueue) StopConsume() {
	q.close <- struct{}{}
	if q.ticker != nil {
		q.ticker.Stop()
	}
}
