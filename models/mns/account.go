package mns

import (
	"fmt"

	. "hezhixiong.im/share/models"

	"hezhixiong.im/share/store/redis"

	"github.com/souriki/ali_mns"
)

type Account struct {
	AccountGuid  string            `json:"account_guid"`  // 公司UUID（唯一标识）
	AccountName  string            `json:"account_name"`  // 公司账号名称
	Client       ali_mns.MNSClient `json:"-"`             // mns 客户端
	DefaultQueue string            `json:"default_queue"` // 默认队列
	Queue        map[string]*Queue `json:"queues"`        // 队列
	Token        string            `json:"token"`         // 安全验证用
}

// --------------------------------------------------------------------------------

func NewAccount() *Account {
	return &Account{}
}

// 获取所有企业账号
func (this *Account) GetAccounts() ([]string, error) {
	return redis.Smembers(REDIS_MNS_ACCOUNTS)
}

func (this *Account) GetSingle(accountGuid string) error {

	// 获取企业账号信息
	arec, err := redis.Hgetall(fmt.Sprintf(REDIS_MNS_ACCOUNT_INFO, accountGuid))
	if err != nil || len(arec) == 0 {
		return fmt.Errorf("Account Is Nil | %v", accountGuid)
	}

	// 获取该企业账号下所有队列，并初始化队列
	queues, err := NewQueue().GetQueues(accountGuid)
	if err != nil || len(queues) == 0 {
		return fmt.Errorf("Account Queues Is Nil | %v", accountGuid)
	}

	queue := make(map[string]*Queue, len(queues)+1)

	for _, queueName := range queues {
		qrec, err := redis.Hgetall(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_INFO, accountGuid, queueName))
		if err != nil || len(qrec) == 0 {
			return fmt.Errorf("Queue Is Nil | [%v] -> [%v]", accountGuid, queueName)
		}

		queue[queueName] = &Queue{
			CallBackErrMaxWaitSec: ConvertInt64(qrec["CallBackErrMaxWaitSec"]),
			CallBackErrWaitSec:    ConvertInt64(qrec["CallBackErrWaitSec"]),
			CallBackUrl:           qrec["CallBackUrl"],
			Enable:                ConvertInt(qrec["Enable"]),
			PubErrMaxWaitSec:      ConvertInt64(qrec["PubErrMaxWaitSec"]),
			PubErrWaitSec:         ConvertInt64(qrec["PubErrWaitSec"]),
			PubMaxDelaySec:        ConvertInt64(qrec["PubMaxDelaySec"]),
			PubMaxMsgNums:         ConvertInt(qrec["PubMaxMsgNums"]),
			QueueName:             queueName,
			SubMaxMsgNums:         ConvertInt(qrec["SubMaxMsgNums"]),
			SubPollingCycleSec:    ConvertInt64(qrec["SubPollingCycleSec"]),
		}
	}

	this.AccountGuid = accountGuid
	this.AccountName = arec["AccountName"]
	this.DefaultQueue = arec["DefaultQueue"]
	this.Token = arec["Token"]
	this.Queue = queue
	this.Client = ali_mns.NewAliMNSClient(arec["EndPoint"], arec["AccessKeyId"], arec["AccessKeySecret"])

	return nil
}

func (this *Account) Insert(accountGuid string, queueName string,
	accountParams map[string]interface{}, queueParams map[string]interface{}) error {

	q := NewQueue()
	if err := q.Insert(accountGuid, queueName, queueParams); err != nil {
		return err
	}

	if err := this.Store(accountGuid, accountParams); err != nil {
		return err
	}

	this.AccountGuid = accountGuid
	this.AccountName = accountParams["AccountName"].(string)
	this.DefaultQueue = accountParams["DefaultQueue"].(string)
	this.Token = accountParams["Token"].(string)
	this.Queue = map[string]*Queue{queueName: q}
	this.Client = ali_mns.NewAliMNSClient(
		accountParams["EndPoint"].(string),
		accountParams["AccessKeyId"].(string),
		accountParams["AccessKeySecret"].(string))

	return nil
}

func (this *Account) Update(accountGuid string, queueName string,
	accountParams map[string]interface{}, queueParams map[string]interface{}) error {

	if this.DefaultQueue != queueName {
		q := NewQueue()
		if err := q.Insert(accountGuid, queueName, queueParams); err != nil {
			return err
		}

		this.Queue[this.DefaultQueue].IsPubStop = true
		this.Queue[this.DefaultQueue].IsSubStop = true
		this.Queue[queueName] = q
	} else {
		if err := this.Queue[queueName].Update(accountGuid, queueName, queueParams); err != nil {
			return err
		}
	}

	if err := this.Store(accountGuid, accountParams); err != nil {
		return err
	}

	this.AccountName = accountParams["AccountName"].(string)
	this.DefaultQueue = accountParams["DefaultQueue"].(string)
	this.Token = accountParams["Token"].(string)
	this.Client = ali_mns.NewAliMNSClient(
		accountParams["EndPoint"].(string),
		accountParams["AccessKeyId"].(string),
		accountParams["AccessKeySecret"].(string))

	return nil
}

func (this *Account) Delete() error {

	// 移除集合缓存
	if err := redis.Srem(REDIS_MNS_ACCOUNTS, []byte(this.AccountGuid)); err != nil {
		return err
	}

	// 删除所有缓冲数据
	keys, err := redis.Keys(fmt.Sprintf(REDIS_MNS_ACCOUNT_ALL, this.AccountGuid))
	if err == nil {
		for _, key := range keys {
			if err = redis.Del(key); err != nil {
				break
			}
		}
	}

	return err
}

// 存储企业账号信息
func (this *Account) Store(accountGuid string, params map[string]interface{}) error {
	err := redis.Sadd(REDIS_MNS_ACCOUNTS, []byte(accountGuid))
	if err != nil {
		return err
	}
	return redis.Hmset(fmt.Sprintf(REDIS_MNS_ACCOUNT_INFO, accountGuid), params)
}
