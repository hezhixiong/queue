package mns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"time"

	. "hezhixiong.im/queue/models"
	. "hezhixiong.im/share/models"

	"hezhixiong.im/queue/models/mns"
	"hezhixiong.im/share/lib"
	"hezhixiong.im/share/logging"
	"hezhixiong.im/share/store/redis"

	"github.com/souriki/ali_mns"
)

type Mns struct{}

type Messages struct {
	Data []ali_mns.MessageSendRequest // 消息列表
	Cap  int                          // 有效消息条数
}

func NewMns() *Mns {
	return &Mns{}
}

// 开启所有推送线程任务
func (this *Mns) PublishAll() {
	if Config.Settings.Mode == MODE_SUBSCRIBE {
		return
	}

	for _, account := range GAccount {
		for _, queue := range account.Queue {
			if queue.Enable == 0 {
				continue
			}

			go func(accountGuid string, queueName string) {
				if err := mns.NewQueue().Recovery(accountGuid, queueName); err != nil {
					logging.Error("Recovery Data | [%v] -> [%v] | %v", accountGuid, queueName, err)
					return
				}

				this.Publish(accountGuid, queueName)
			}(account.AccountGuid, queue.QueueName)
		}
	}
}

// 开启所有订阅线程任务
func (this *Mns) SubscribeAll() {
	if Config.Settings.Mode == MODE_PUBLISH {
		return
	}

	for _, account := range GAccount {
		for _, queue := range account.Queue {
			if queue.Enable != 0 {
				go this.Subscribe(account.AccountGuid, queue.QueueName)
			}
		}
	}
}

func (this *Mns) CreateSimpleQueue(queueName string, client ali_mns.MNSClient) error {
	return this.CreateQueue(queueName, 0, 65536, 345600, 30, 0, client)
}

func (this *Mns) CreateSimpleQueueByArgs(queueName string, endPoint string,
	accessKeyId string, accessKeySecret string) error {

	client := ali_mns.NewAliMNSClient(endPoint, accessKeyId, accessKeySecret)
	return this.CreateSimpleQueue(queueName, client)
}

func (this *Mns) CreateQueue(queueName string, delaySeconds int32, maxMessageSize int32,
	messageRetentionPeriod int32, visibilityTimeout int32,
	pollingWaitSeconds int32, client ali_mns.MNSClient) error {

	mnsQueueManager := ali_mns.NewMNSQueueManager(client)
	err := mnsQueueManager.CreateQueue(
		queueName, delaySeconds, maxMessageSize, messageRetentionPeriod, visibilityTimeout, pollingWaitSeconds,
	)
	if err != nil && ali_mns.ERR_MNS_QUEUE_ALREADY_EXIST.IsEqual(err) {
		mnsQueueManager.SetQueueAttributes(
			queueName, delaySeconds, maxMessageSize, messageRetentionPeriod, visibilityTimeout, pollingWaitSeconds,
		)
		return nil
	}
	if err != nil && !ali_mns.ERR_MNS_QUEUE_ALREADY_EXIST_AND_HAVE_SAME_ATTR.IsEqual(err) {
		return err
	}

	return nil
}

// 发布消息
func (this *Mns) Publish(accountGuid string, queueName string) {

	if GAccount[accountGuid] == nil ||
		GAccount[accountGuid].Queue[queueName] == nil ||
		GAccount[accountGuid].Queue[queueName].IsPubStop ||
		GAccount[accountGuid].Queue[queueName].IsPubIn {
		return
	}

	logging.Debug("Start To Publish [%v] -> [%v]", accountGuid, queueName)

	queue := GAccount[accountGuid].Queue[queueName]
	queue.IsPubIn = true
	isNextMsg := make(chan struct{})
	msgData := make(chan ali_mns.MessageSendRequest)
	ticker := time.NewTicker(time.Duration(queue.PubMaxDelaySec) * time.Second)
	src := fmt.Sprintf(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST, accountGuid, queueName))
	dest := fmt.Sprintf(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST_BAK, accountGuid, queueName))

	defer func() {
		queue.IsPubIn = false
	}()

	// 获取客户发送的消息
	go func() {
		for {
			data, err := redis.Do("BRPOPLPUSH", src, dest, 0)
			if queue.IsPubStop && string(data.([]byte)) == EXIT_BRPOPLPUSH_MSG {
				queue.Delete(accountGuid, queueName, true)
				logging.Debug("Exit BRPOPLPUSH | [%v] -> [%v]", accountGuid, queueName)
				logging.Debug("Stop To Publish | [%v] -> [%v]", accountGuid, queueName)
				return
			}
			if err != nil {
				logging.Debug("BRPOPLPUSH | [%v] -> [%v] | %v", accountGuid, queueName, err)
				continue
			}

			// 发送处理新消息
			msgData <- ali_mns.MessageSendRequest{
				MessageBody:  string(data.([]byte)),
				DelaySeconds: 0,
				Priority:     8,
			}

			// 等待上一条处理完成
			isNextMsg <- struct{}{}
		}
	}()

	// 已接收消息列表
	msgs := Messages{
		Data: make([]ali_mns.MessageSendRequest, queue.PubMaxMsgNums),
		Cap:  0,
	}

	for {
		if queue.IsPubStop {
			redis.Lpush(src, []byte(EXIT_BRPOPLPUSH_MSG))
			return
		}

		select {
		case msg := <-msgData:

			// 数据发送
			if msgs.Cap >= queue.PubMaxMsgNums && queue.IsPubSending == false {
				sendmsgs := msgs.Data[:]
				msgs.Cap = 0
				this.sendmessage(accountGuid, queueName, sendmsgs)
			}

			// 等待发送完成
			for queue.IsPubSending {
				time.Sleep(1 * time.Second)
			}

			// 记录有效数据
			msgs.Data[msgs.Cap] = msg
			msgs.Cap++

			// 可以发送下一条数据了
			<-isNextMsg

		case <-ticker.C:
			if msgs.Cap > 0 && queue.IsPubSending == false {
				sendmsgs := msgs.Data[:msgs.Cap]
				msgs.Cap = 0
				this.sendmessage(accountGuid, queueName, sendmsgs)
			}
		}
	}
}

// 订阅消息
func (this *Mns) Subscribe(accountGuid string, queueName string) {

	if GAccount[accountGuid] == nil ||
		GAccount[accountGuid].Queue[queueName] == nil ||
		GAccount[accountGuid].Queue[queueName].IsSubStop ||
		GAccount[accountGuid].Queue[queueName].IsSubIn {
		return
	}

	logging.Debug("Start To Subscribe [%v] -> [%v]", accountGuid, queueName)

	queue := GAccount[accountGuid].Queue[queueName]
	accountName := GAccount[accountGuid].AccountName
	queue.IsSubIn = true
	mnsQueue := ali_mns.NewMNSQueue(queueName, GAccount[accountGuid].Client)
	respChan := make(chan ali_mns.BatchMessageReceiveResponse)
	errChan := make(chan error)
	endChan := make(chan struct{})
	isSendEmail := false

	defer func() {
		queue.IsSubIn = false
	}()

	var errCount int64
	delIdch := NewChannel(1)

	var deleteAliMsgs = func(ids []interface{}) {
		idnum := len(ids)
		strids := make([]string, idnum)
		for i := 0; i < idnum; i++ {
			strids[i] = ids[i].(string)
		}
		start, end := 0, 0
		for i := 0; i < idnum; i = end {
			start = i
			end = i + queue.PubMaxMsgNums
			if end > idnum {
				end = idnum
			}
			count := 0
			var err error
			for ; count < 10; count++ {
				if _, err = mnsQueue.BatchDeleteMessage(strids[start:end]...); err != nil {
					logging.Debug("BatchDeleteMessage  count = %d | %v", count, err)
				} else {
					break
				}
			}
			if count == 10 {
				logging.Error("BatchDeleteMessage  count = %d | %v", count, err)
			}
		}
	}

	go func() {
		for {
			select {
			case <-endChan:
				deleteAliMsgs(delIdch.Get())
				return
			case <-delIdch.FullChan:
				deleteAliMsgs(delIdch.Get())
			}
		}
	}()

	go func() {
		for {
			select {
			case resp := <-respChan:
				handlers := make([]interface{}, len(resp.Messages))
				for i, message := range resp.Messages {
					handlers[i] = message.ReceiptHandle
				}

				for {
					if err := this.callback(accountGuid, accountName, queueName, resp.Messages); err != nil {
						errCount++
						timedelay := errCount * queue.CallBackErrWaitSec

						if timedelay > queue.CallBackErrMaxWaitSec {
							if !isSendEmail {
								err := this.warning(accountGuid, queueName, "hezhixiong_Queue 回调失败")
								if err != nil {
									logging.Error("Send Email | ", err)
								} else {
									isSendEmail = true
								}
							}

							timedelay = queue.CallBackErrMaxWaitSec

							if Config.Settings.IsDiscard == 1 {
								logging.Error("Callback Err, Discard Data")
								delIdch.Add(handlers...)
								errCount = 0
								break
							}
						}

						logging.Error("Callback [%v] -> [%v] | %v | %v | %+v",
							accountGuid, queueName, errCount, err, resp.Messages)

						// 回调失败，采用延时递增再发机制，直至回调成功
						time.Sleep(time.Second * time.Duration(timedelay))
						continue
					}

					// 回调成功，退出回调循环，但是不退出接收消息循环
					isSendEmail = false
					errCount = 0

					delIdch.Add(handlers...)

					logging.Info("Callback Success | [%v] -> [%v] | %+v", accountGuid, queueName, resp.Messages)
					break
				}

			case err, ok := <-errChan:
				if ali_mns.ERR_MNS_QUEUE_NOT_EXIST.IsEqual(err) || ali_mns.ERR_MNS_INVALID_QUEUE_NAME.IsEqual(err) {
					if !ok {
						close(errChan)
					}
					close(respChan)
					logging.Error("Subscribe [%v] -> [%v] | %v", accountGuid, queueName, err)
					return
				}

			case <-endChan:
				close(errChan)
				close(respChan)
				return
			}
		}
	}()

	for {
		if queue.IsSubStop {
			close(endChan)
			queue.Delete(accountGuid, queueName, false)
			logging.Debug("Stop To Subscribe [%v] -> [%v]", accountGuid, queueName)
			return
		}

		mnsQueue.BatchReceiveMessage(respChan, errChan, int32(queue.SubMaxMsgNums), int64(queue.SubPollingCycleSec))
	}
}

// --------------------------------------------------------------------------------

func (this *Mns) sendmessage(accountGuid string, queueName string, msgs []ali_mns.MessageSendRequest) {
	if GAccount[accountGuid] == nil || GAccount[accountGuid].Queue[queueName] == nil || len(msgs) == 0 {
		return
	}

	queue := GAccount[accountGuid].Queue[queueName]
	mnsQueue := ali_mns.NewMNSQueue(queueName, GAccount[accountGuid].Client)
	queue.IsPubSending = true // 同一队列同一时间只允许有一个发送
	isSendEmail := false      // 邮件提醒仅发一次

	var errCount int64
	defer func() {
		queue.IsPubSending = false
	}()

	// 采用延时递增再发，直至发生成功
	for {
		if queue.IsPubStop {
			logging.Debug("Stop To Publish [%v] -> [%v] | %v", accountGuid, queueName, msgs)
			return
		}

		if _, err := mnsQueue.BatchSendMessage(msgs...); err != nil {
			errCount++
			timedelay := errCount * queue.PubErrWaitSec

			if timedelay > queue.PubErrMaxWaitSec {
				if !isSendEmail {
					err := this.warning(accountGuid, queueName, "hezhixiong_Queue 发布失败")
					if err != nil {
						logging.Error("Send Email | ", err)
					} else {
						isSendEmail = true
					}
				}

				timedelay = queue.PubErrMaxWaitSec
			}

			logging.Error("Send To MNS [%v] -> [%v] | [%v] | %v | %v",
				accountGuid, queueName, errCount, err, msgs)
			time.Sleep(time.Second * time.Duration(timedelay))
			continue
		}

		break
	}

	redis.Del(fmt.Sprintf(REDIS_MNS_ACCOUNT_QUEUE_LIST_BAK, accountGuid, queueName))
	logging.Debug("Send To MNS Success | [%v] -> [%v] | %v", accountGuid, queueName, msgs)
}

func (this *Mns) callback(accountGuid string, accountName string, queueName string, data []ali_mns.MessageReceiveResponse) error {

	// 回调数据格式封装
	bodyJson := struct {
		AccountGuid string                           `json:"account_guid"`
		AccountName string                           `json:"account_name"`
		QueueName   string                           `json:"queue_name"`
		Data        []ali_mns.MessageReceiveResponse `json:"data"`
	}{
		AccountGuid: accountGuid,
		AccountName: accountName,
		QueueName:   queueName,
		Data:        data,
	}
	b, err := json.Marshal(bodyJson)
	if err != nil {
		return err
	}

	// 向客户端回调
	resp, err := http.Post(
		GAccount[accountGuid].Queue[queueName].CallBackUrl,
		"application/json;charset=utf-8",
		bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 验证客户端接收是否成功
	ret, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	check := struct {
		Code int `json:"code"`
	}{}
	if err := json.Unmarshal(ret, &check); err != nil {
		logging.Debug("response_body = %v", string(ret))
		return err
	}
	if check.Code != 200 {
		logging.Debug("response_body = %v", string(ret))
		return fmt.Errorf("Callback Messages To Client, Client Response [code:%v]", check.Code)
	}

	return nil
}

func (this *Mns) warning(accountGuid string, queueName string, subject string) error {
	html, err := template.ParseFiles(TEMPLATE_MAIL_MNS_WARNING)
	if err != nil {
		return err
	}

	var data bytes.Buffer
	localIPAddr, _ := lib.GetLocalIPAddr()
	err = html.Execute(&data, map[string]string{
		"AccountName": accountGuid,
		"QueueName":   queueName,
		"LocalIPAddr": localIPAddr,
		"Datetime":    time.Now().Format("2006-01-02 15:04:05"),
	})
	if err != nil {
		return err
	}

	ec := lib.EmailConfig{
		Server:   Config.Email.Server,
		Port:     Config.Email.Port,
		Addr:     Config.Email.Addr,
		Password: Config.Email.Password,
		Subject:  subject,
		To:       Config.Settings.SupportEmail,
		Html:     data.String(),
	}

	return lib.SendMail(ec)
}
