# DelayQueue
# 延迟队列
这是一个使用Go语言和Redis实现的延迟队列。它使用Redis的有序集合和列表数据结构来存储和处理消息。
## 使用方法
首先，创建一个新的延迟队列对象：
queue := NewDelayQueue("queue_name", redisClient, callback)
其中， `queue_name` 是队列的名称， `redisClient` 是已经初始化好的Redis客户端， `callback` 是一个处理消息的回调函数。
然后，可以使用以下方法向队列中添加消息：
queue.SendScheduleMsg("message", time.Now().Add(10*time.Second))
这将在10秒后将消息"message"添加到队列中。
或者使用以下方法添加延迟消息：
queue.SendDelayMsg("message", 10*time.Second)
这将在10秒后将消息"message"添加到队列中。
可以使用以下方法开始消费消息：
done := queue.StartConsume()
这将启动一个新的协程来消费消息。可以使用  `<-done` 来让消费者等待。
可以使用以下方法停止消费消息：
queue.StopConsume()
这将停止消费者协程。
## 配置
可以使用以下方法来配置队列：
-  `WithLogger(logger *log.Logger)` : 设置日志记录器。
-  `WithFetchInterval(d time.Duration)` : 设置从Redis中拉取消息的时间间隔。
-  `WithMaxConsumeDuration(d time.Duration)` : 设置消息的超时时间。如果在消息传递后的这段时间内未收到确认，DelayQueue将尝试再次传递此消息。
-  `WithFetchLimit(limit uint)` : 设置单次拉取消息的数量。
## 注意事项
- 队列名称必须在Redis中是唯一的。
- 回调函数应该处理消息并返回一个布尔值，表示是否应该确认消息。如果返回true，消息将被确认并从队列中删除。如果返回false，消息将被视为未确认，并可能在以后被重试。
- 在调用 `StopConsume` 后，不应再使用队列对象。如果需要，应该创建一个新的队列对象。