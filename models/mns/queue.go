package mns

import (
	"fmt"
	"strings"

	"hezhixiong.im/queue/models/store"
	. "hezhixiong.im/share/models"
	"hezhixiong.im/share/store/redis"
)

type Queue struct {
	CallBackErrMaxWaitSec int64  `json:"call_back_err_max_wait_sec"` // 向客户端回调失败最大等待时间（秒）
	CallBackErrWaitSec    int64  `json:"call_back_err_wait_sec"`     // 向客户端回调失败需要等待的时间（秒）
	CallBackUrl           string `json:"call_back_url"`              // 向客户端回调Url
	Enable                int    `json:"enable"`                     // 发布订阅消息开关
	IsPubIn               bool   `json:"-"`                          // 发布中否
	IsPubSending          bool   `json:"-"`                          // 发布中否
	IsPubStop             bool   `json:"-"`                          // 停止发布
	IsSendEmail           bool   `json:"-"`                          // Redis存储失败后是否发送了邮件提醒
	IsSubIn               bool   `json:"-"`                          // 订阅中否
	IsSubStop             bool   `json:"-"`                          // 停止订阅
	PubErrMaxWaitSec      int64  `json:"pub_err_max_wait_sec"`       // 发布失败最大等待时间（秒）
	PubErrWaitSec         int64  `json:"pub_err_wait_sec"`           // 发布失败需要等待的时间（秒）
	PubMaxDelaySec        int64  `json:"pub_max_delay_sec"`          // 发布消息最大延时（秒）
	PubMaxMsgNums         int    `json:"pub_max_msg_nums"`           // 批量发布最大数
	QueueName             string `json:"queue_name"`                 // 队列名称
	SubMaxMsgNums         int    `json:"sub_max_msg_nums"`           // 批量订阅最大消息数
	SubPollingCycleSec    int64  `json:"sub_polling_cycle_sec"`      // 订阅轮询时间间隔（秒）
}

// --------------------------------------------------------------------------------

func NewQueue() *Queue {
	return &Queue{}
}

// 获取所有消息队列
func (this *Queue) GetQueues(accountGuid string) ([]string, error) {
	return redis.Smembers(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUES, accountGuid))
}

func (this *Queue) GetSingle(accountGuid string, queueName string) error {
	qrec, err := redis.Hgetall(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_INFO, accountGuid, queueName))
	if err != nil || len(qrec) == 0 {
		return fmt.Errorf("Queue Is Nil | [%v] -> [%v]", accountGuid, queueName)
	}

	this.CallBackErrMaxWaitSec = ConvertInt64(qrec["CallBackErrMaxWaitSec"])
	this.CallBackErrWaitSec = ConvertInt64(qrec["CallBackErrWaitSec"])
	this.CallBackUrl = qrec["CallBackUrl"]
	this.Enable = ConvertInt(qrec["Enable"])
	this.PubErrMaxWaitSec = ConvertInt64(qrec["PubErrMaxWaitSec"])
	this.PubErrWaitSec = ConvertInt64(qrec["PubErrWaitSec"])
	this.PubMaxDelaySec = ConvertInt64(qrec["PubMaxDelaySec"])
	this.PubMaxMsgNums = ConvertInt(qrec["PubMaxMsgNums"])
	this.QueueName = queueName
	this.SubMaxMsgNums = ConvertInt(qrec["SubMaxMsgNums"])
	this.SubPollingCycleSec = ConvertInt64(qrec["SubPollingCycleSec"])

	return nil
}

func (this *Queue) Insert(accountGuid string, queueName string, params map[string]interface{}) error {
	return this.Update(accountGuid, queueName, params)
}

func (this *Queue) Update(accountGuid string, queueName string, params map[string]interface{}) error {
	if err := this.Store(accountGuid, queueName, params); err != nil {
		return err
	}

	this.CallBackErrMaxWaitSec = params["CallBackErrMaxWaitSec"].(int64)
	this.CallBackErrWaitSec = params["CallBackErrWaitSec"].(int64)
	this.CallBackUrl = params["CallBackUrl"].(string)
	this.Enable = params["Enable"].(int)
	this.PubErrMaxWaitSec = params["PubErrMaxWaitSec"].(int64)
	this.PubErrWaitSec = params["PubErrWaitSec"].(int64)
	this.PubMaxDelaySec = params["PubMaxDelaySec"].(int64)
	this.PubMaxMsgNums = params["PubMaxMsgNums"].(int)
	this.QueueName = queueName
	this.SubMaxMsgNums = params["SubMaxMsgNums"].(int)
	this.SubPollingCycleSec = params["SubPollingCycleSec"].(int64)

	return nil
}

// 将备份消息还原至消息队列
func (this *Queue) Recovery(accountGuid string, queueName string) error {
	var err error

	if err = this.recoverySqlite(accountGuid, queueName); err == nil {
		this.recoveryRedis(accountGuid, queueName)
	}

	return err
}

// 保存消息至消息队列
func (this *Queue) SaveMessage(accountGuid string, queueName string, message []byte) error {
	return redis.Lpush(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST, accountGuid, queueName), message)
}

// 更新消息队列账号信息
func (this *Queue) Store(accountGuid string, queueName string, params map[string]interface{}) error {
	err := redis.Sadd(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUES, accountGuid), []byte(queueName))
	if err != nil {
		return err
	}

	return redis.Hmset(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_INFO, accountGuid, queueName), params)
}

// 删除消息队列账号信息
func (this *Queue) Delete(accountGuid string, queueName string, isPub bool) error {
	redis.Srem(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUES, accountGuid), []byte(queueName))
	redis.Del(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_INFO, accountGuid, queueName))

	if isPub {
		redis.Del(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST, accountGuid, queueName))
		redis.Del(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST_BAK, accountGuid, queueName))
	}

	return nil
}

// ----------------------------------------------------------------------------

func (this *Queue) recoverySqlite(accountGuid string, queueName string) error {
	var ids string
	var limit uint64 = 1000

	sqlite := store.NewSqlite()
	list := fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST, accountGuid, queueName)

	for {
		data, err := sqlite.GetLimits(accountGuid, queueName, limit)
		if err != nil {
			return err
		}

		for _, v := range data {
			redis.Rpush(list, []byte(v.Message))
			ids = fmt.Sprintf("%s,%d", ids, v.ID)
		}

		if _, err := sqlite.DeleteByIds(strings.TrimLeft(ids, ",")); err != nil {
			return err
		}

		if len(data) < int(limit) {
			break
		}
	}

	return nil
}

func (this *Queue) recoveryRedis(accountGuid string, queueName string) {
	list := fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST, accountGuid, queueName)
	listBack := fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST_BAK, accountGuid, queueName)

	msgs, err := redis.LRange(listBack, 0, -1)
	if err != nil || len(msgs) == 0 {
		return
	}

	for _, msg := range msgs {
		redis.Rpush(list, []byte(msg))
		redis.Lrem(listBack, 1, []byte(msg))
	}
}
