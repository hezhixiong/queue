package mns

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	. "hezhixiong.im/queue/models"

	"hezhixiong.im/queue/models/mns"
	"hezhixiong.im/queue/models/store"
	"hezhixiong.im/share/lib"
	"hezhixiong.im/share/lib/crypto"
	"hezhixiong.im/share/logging"

	"fmt"

	"github.com/gin-gonic/gin"
)

type Queue struct{}

func NewQueue() *Queue {
	return &Queue{}
}

func (this *Queue) PutOrganize(c *gin.Context) {
	var _params struct {
		AccountGuid           string `json:"account_guid" binding:"required"`
		AccountName           string `json:"account_name" binding:"required"`
		AccessKeyId           string `json:"access_key_id" binding:"required"`
		AccessKeySecret       string `json:"access_key_secret" binding:"required"`
		EndPoint              string `json:"end_point" binding:"required"`
		QueueName             string `json:"queue_name" binding:"required"`
		Enable                int    `json:"enable" binding:"gte=0,lte=1"`
		PubMaxMsgNums         int    `json:"pub_max_msg_nums" binding:"gte=0,lte=16"`
		PubMaxDelaySec        int64  `json:"pub_max_delay_sec" binding:"gte=0,lte=3600"`
		PubErrWaitSec         int64  `json:"pub_err_wait_sec" binding:"gte=0,lte=3600"`
		PubErrMaxWaitSec      int64  `json:"pub_err_max_wait_sec" binding:"gte=0,lte=3600"`
		SubMaxMsgNums         int    `json:"sub_max_msg_nums" binding:"gte=0,lte=16"`
		SubPollingCycleSec    int64  `json:"sub_polling_cycle_sec" binding:"gte=0,lte=30"`
		CallBackErrWaitSec    int64  `json:"call_back_err_wait_sec" binding:"gte=0,lte=3600"`
		CallBackErrMaxWaitSec int64  `json:"call_back_err_max_wait_sec" binding:"gte=0,lte=3600"`
		CallBackUrl           string `json:"call_back_url"`
		Timestamp             string `json:"timestamp" binding:"required"`
		Token                 string `json:"token" binding:"required"`
		Signature             string `json:"signature" binding:"required"`
	}

	if err := c.BindJSON(&_params); err != nil {
		logging.Debug("Parsing Json | %v", err)
		lib.WriteString(c, 44001, nil)
		return
	}

	if Config.Settings.Mode != MODE_PUBLISH && _params.Enable > 0 && len(_params.CallBackUrl) == 0 {
		logging.Debug("Subscribe CallBackUrl Shound Not Nil.")
		lib.WriteString(c, 44001, nil)
		return
	}
	_params.EndPoint = strings.TrimSpace(_params.EndPoint)

	// 校验域名格式参数
	pieces := strings.Split(_params.EndPoint, ".")
	if len(pieces) != 5 {
		lib.WriteString(c, 40004, nil)
		return
	}

	// 校验时间戳
	timestamp, _ := strconv.ParseInt(_params.Timestamp, 10, 64)
	if !this.checkTimestamp(timestamp, TIME_STAMP_TOLERANCE) {
		lib.WriteString(c, 44016, nil)
		return
	}

	// 校验队列名称
	if !this.checkQueueName(_params.QueueName) {
		lib.WriteString(c, 43755, nil)
		return
	}

	// 校验签名
	content := strings.Join(
		[]string{
			_params.AccountGuid,
			_params.AccountName,
			_params.AccessKeyId,
			_params.AccessKeySecret,
			_params.EndPoint,
			_params.QueueName,
			_params.Timestamp,
		}, ":")
	signature := crypto.HmacSignature(content, _params.Token)
	if !strings.EqualFold(_params.Signature, signature) {
		lib.WriteString(c, 40010, nil)
		return
	}

	// 参数默认值设置
	if _params.PubMaxMsgNums == 0 {
		_params.PubMaxMsgNums = DEF_PUB_MAX_MSG_NUMS
	}
	if _params.PubMaxDelaySec == 0 {
		_params.PubMaxDelaySec = int64(DEF_PUB_MAX_DELAY_SEC)
	}
	if _params.PubErrWaitSec == 0 {
		_params.PubErrWaitSec = int64(DEF_PUB_ERR_WAIT_SEC)
	}
	if _params.PubErrMaxWaitSec == 0 {
		_params.PubErrMaxWaitSec = int64(DEF_PUB_ERR_MAX_WAIT_SEC)
	}
	if _params.SubMaxMsgNums == 0 {
		_params.SubMaxMsgNums = DEF_SUB_MAX_MSG_NUMS
	}
	if _params.SubPollingCycleSec == 0 {
		_params.SubPollingCycleSec = int64(DEF_SUB_POLLING_CYCLE_SEC)
	}
	if _params.CallBackErrWaitSec == 0 {
		_params.CallBackErrWaitSec = int64(DEF_CALLBACK_ERR_WAIT_SEC)
	}
	if _params.CallBackErrMaxWaitSec == 0 {
		_params.CallBackErrMaxWaitSec = int64(DEF_CALLBACK_ERR_MAX_WAIT_SEC)
	}

	accountParams := map[string]interface{}{
		"AccountName":     _params.AccountName,
		"AccessKeyId":     _params.AccessKeyId,
		"AccessKeySecret": _params.AccessKeySecret,
		"EndPoint":        _params.EndPoint,
		"DefaultQueue":    _params.QueueName,
		"Token":           _params.Token,
	}

	queueParams := map[string]interface{}{
		"CallBackErrMaxWaitSec": _params.CallBackErrMaxWaitSec,
		"CallBackErrWaitSec":    _params.CallBackErrWaitSec,
		"CallBackUrl":           _params.CallBackUrl,
		"Enable":                _params.Enable,
		"PubErrMaxWaitSec":      _params.PubErrMaxWaitSec,
		"PubErrWaitSec":         _params.PubErrWaitSec,
		"PubMaxDelaySec":        _params.PubMaxDelaySec,
		"PubMaxMsgNums":         _params.PubMaxMsgNums,
		"SubMaxMsgNums":         _params.SubMaxMsgNums,
		"SubPollingCycleSec":    _params.SubPollingCycleSec,
	}

	//校验名字，名字已存在且不是该组织
	if GNameIndex[_params.AccountName] != "" && GNameIndex[_params.AccountName] != _params.AccountGuid {
		logging.Error(" AccountName is exist %v | %v", _params.AccountGuid, _params.AccountName)
		lib.WriteString(c, 40002, nil)
		return
	}

	// 校验企业Guid 和名字
	if GAccount[_params.AccountGuid] == nil {
		a, err := this.createAccount(_params.AccountGuid, _params.QueueName, accountParams, queueParams)
		if err != nil {
			logging.Error("Create Account  %v | %v", _params.AccountGuid, err)
			lib.WriteString(c, 40002, nil)
			return
		}

		GAccount[_params.AccountGuid] = a

		GNameIndex[_params.AccountName] = _params.AccountGuid

	} else {
		err := this.updateAccount(_params.AccountGuid, _params.QueueName, accountParams, queueParams)
		if err != nil {
			logging.Error("Update Account | %v | %v", _params.AccountGuid, err)
			lib.WriteString(c, 40002, nil)
			return
		}

		GNameIndex[_params.AccountName] = _params.AccountGuid
	}

	alimns := NewMns()

	// 开启·关闭发布
	if Config.Settings.Mode != MODE_SUBSCRIBE && _params.Enable != 0 {
		if !GAccount[_params.AccountGuid].Queue[_params.QueueName].IsPubIn {
			go func(aguid string, qname string) {
				GAccount[aguid].Queue[qname].IsPubStop = false
				time.Sleep(time.Duration(GAccount[aguid].Queue[qname].PubMaxDelaySec) * time.Second)
				alimns.Publish(aguid, qname)
			}(_params.AccountGuid, _params.QueueName)
		}
	} else {
		GAccount[_params.AccountGuid].Queue[_params.QueueName].IsPubStop = true
	}

	// 开启·关闭订阅
	if Config.Settings.Mode != MODE_PUBLISH && _params.Enable != 0 {
		if !GAccount[_params.AccountGuid].Queue[_params.QueueName].IsSubIn {
			go func(aguid string, qname string) {
				GAccount[aguid].Queue[qname].IsSubStop = false
				time.Sleep(time.Duration(GAccount[aguid].Queue[qname].SubPollingCycleSec) * time.Second)
				alimns.Subscribe(aguid, qname)
			}(_params.AccountGuid, _params.QueueName)
		}
	} else {
		GAccount[_params.AccountGuid].Queue[_params.QueueName].IsSubStop = true
	}

	lib.WriteString(c, 200, nil)
}

func (this *Queue) DelOrganize(c *gin.Context) {
	var _params struct {
		AccountGuid string `json:"account_guid" binding:"required"`
		Timestamp   string `json:"timestamp" binding:"required"`
		Signature   string `json:"signature" binding:"required"`
	}

	if err := c.BindJSON(&_params); err != nil {
		logging.Debug("%+v", err)
		lib.WriteString(c, 44001, nil)
		return
	}

	// 校验时间戳
	timestamp, _ := strconv.ParseInt(_params.Timestamp, 10, 64)
	if !this.checkTimestamp(timestamp, 15) {
		lib.WriteString(c, 44016, nil)
		return
	}

	// 校验企业
	if GAccount[_params.AccountGuid] == nil {
		lib.WriteString(c, 43752, nil)
		return
	}

	// 校验签名
	content := strings.Join(
		[]string{
			_params.AccountGuid,
			_params.Timestamp,
		}, ":")
	signature := crypto.HmacSignature(content, GAccount[_params.AccountGuid].Token)
	if !strings.EqualFold(_params.Signature, signature) {
		lib.WriteString(c, 40010, nil)
		return
	}

	this.deleteAccount(_params.AccountGuid)

	lib.WriteString(c, 200, nil)
}

// 推送消息
func (this *Queue) SendMessage(c *gin.Context) {

	// 发布消息未开启
	if Config.Settings.Mode == MODE_SUBSCRIBE {
		lib.WriteString(c, 43751, nil)
		return
	}
	var _params struct {
		AccountName  string `json:"account_name" binding:"required"`
		QueueName    string `json:"queue_name" binding:"required"`
		Body         string `json:"body" binding:"required"`
		DelaySeconds int64  `json:"delay_seconds" binding:"gte=0,lte=604800"`
		Priority     int64  `json:"priority" binding:"gte=0,lte=16"`
		Timestamp    string `json:"timestamp" binding:"required"`
		Signature    string `json:"signature" binding:"required"`
	}
	if err := c.BindJSON(&_params); err != nil {
		logging.Debug("Parsing Json | %v", err)
		lib.WriteString(c, 44001, nil)
		return
	}

	// 校验时间戳
	timestamp, _ := strconv.ParseInt(_params.Timestamp, 10, 64)
	if !this.checkTimestamp(timestamp, 15) {
		lib.WriteString(c, 44016, nil)
		return
	}

	// 校验公司是否存在
	account := GAccount[GNameIndex[_params.AccountName]]
	if account == nil {
		logging.Error("Publish Organize is Nil | %v", _params.AccountName)
		lib.WriteString(c, 43752, nil)
		return
	}

	// 校验队列账号
	if account.Queue[_params.QueueName] == nil {
		logging.Error("Publish [%v] -> [%v] is nil.", _params.AccountName, _params.QueueName)
		lib.WriteString(c, 43753, nil)
		return
	}

	// 校验签名
	content := strings.Join([]string{_params.AccountName, _params.QueueName, _params.Timestamp}, ":")
	signature := crypto.HmacSignature(content, GAccount[_params.AccountName].Token)
	if !strings.EqualFold(_params.Signature, signature) {
		logging.Debug("Server Signature | %v", signature)
		lib.WriteString(c, 40010, nil)
		return
	}

	// 保存消息数据
	err := mns.NewQueue().SaveMessage(_params.AccountName, _params.QueueName, []byte(_params.Body))
	if err != nil {
		logging.Error("Save Message To Redis | %v", err)
		lib.WriteString(c, 40002, nil)
		return
	}
	lib.WriteString(c, 200, nil)
}

/**
 * 函数名称：Exchange（微信代理·发布消息）
 * 功能说明：Exchange 是从微信客户端接收数据并向阿里的消息服务（mns）进行推送数据。
 */
func (this *Queue) Exchange(c *gin.Context) {

	// 发布消息未开启
	if Config.Settings.Mode == MODE_SUBSCRIBE {
		lib.WriteString(c, 43751, nil)
		return
	}

	var _params struct {
		ClientCode string      `json:"client_code"`
		Data       interface{} `json:"data" binding:"required"`
		Event      string      `json:"event" binding:"required"`
		Organize   string      `json:"organize" binding:"required"`
		UserTicket string      `json:"user_ticket" binding:"required"`
		Timestamp  string      `json:"timestamp" binding:"required"`
		Signature  string      `json:"signature" binding:"required"`
	}

	if err := c.BindJSON(&_params); err != nil {
		logging.Debug("Parsing Json | %v", err)
		lib.WriteString(c, 44001, nil)
		return
	}

	// 校验时间戳
	timestamp, _ := strconv.ParseInt(_params.Timestamp, 10, 64)
	if !this.checkTimestamp(timestamp, TIME_STAMP_TOLERANCE) {
		lib.WriteString(c, 44016, nil)
		return
	}

	// 校验公司是否存在
	account := GAccount[GNameIndex[_params.Organize]]
	if account == nil {
		logging.Error("Publish Organize is Nil | %v", _params.Organize)
		lib.WriteString(c, 43752, nil)
		return
	}

	// 校验签名
	content := strings.Join(
		[]string{
			_params.ClientCode,
			_params.Event,
			_params.Organize,
			_params.UserTicket,
			_params.Timestamp,
		}, ":")
	signature := crypto.HmacSignature(content, account.Token)
	if !strings.EqualFold(_params.Signature, signature) {
		lib.WriteString(c, 40010, nil)
		return
	}

	// 校验队列是否存在
	q := account.Queue[account.DefaultQueue]
	if q == nil {
		logging.Error("Publish QueueName Is Nil | [%v] -> [%v]", _params.Organize, account.DefaultQueue)
		lib.WriteString(c, 43753, nil)
		return
	}

	// 队列发布消息未开启
	if q.Enable == 0 {
		lib.WriteString(c, 43751, nil)
		return
	}

	// 发送数据包：map -> string
	params := map[string]interface{}{
		"ClientCode": _params.ClientCode,
		"Data":       _params.Data,
		"Event":      _params.Event,
		"Organize":   _params.Organize,
		"UserTicket": _params.UserTicket,
		"Timestamp":  _params.Timestamp,
	}

	message, err := json.Marshal(params)
	if err != nil {
		logging.Error("Parsing Json | %v", err)
		lib.WriteString(c, 44001, nil)
		return
	}

	queue := mns.NewQueue()
	if err := queue.SaveMessage(account.AccountGuid, account.DefaultQueue, message); err != nil {
		if !q.IsSendEmail {
			logging.Error("Save Message To Redis | [%v] -> [%v] | %v", _params.Organize, account.DefaultQueue, err)
			err := NewMns().warning(_params.Organize, account.DefaultQueue, "hezhixiong_Queue Redis 存储失败")
			if err != nil {
				logging.Error("Send Email | ", err)
			} else {
				q.IsSendEmail = true
			}
		}
		if _, err := store.NewSqlite().Insert(account.AccountGuid, account.DefaultQueue, message); err != nil {
			logging.Error("Save Message To SqliteDB | %v", err)
			lib.WriteString(c, 40002, nil)
			return
		}
	}

	lib.WriteString(c, 200, nil)
}

// --------------------------------------------------------------------------------

func (this *Queue) checkTimestamp(timestamp int64, minute int64) bool {
	nowTimestamp := time.Now().Unix()
	if nowTimestamp-timestamp > -60*minute && nowTimestamp-timestamp < 60*minute {
		return true
	}
	return false
}

/**
 * 队列名规则:
 * 不能大于256字符
 * 首字母只能是字母或数字
 * 剩下的只能是字母或数字或中横杠(-)
 */
func (this *Queue) checkQueueName(queueName string) bool {
	pattern := `^[a-z|A-Z|0-9][a-z|A-Z|0-9|-]{0,255}$`
	return regexp.MustCompile(pattern).MatchString(queueName)
}

func (this *Queue) createAccount(accountGuid string, queueName string,
	accountParams map[string]interface{}, queueParams map[string]interface{}) (*mns.Account, error) {

	err := NewMns().CreateSimpleQueueByArgs(queueName, accountParams["EndPoint"].(string),
		accountParams["AccessKeyId"].(string), accountParams["AccessKeySecret"].(string))
	if err != nil {
		return nil, err
	}

	a := mns.NewAccount()
	err = a.Insert(accountGuid, queueName, accountParams, queueParams)
	if err != nil {
		return nil, err
	}

	return a, nil
}

func (this *Queue) updateAccount(accountGuid string, queueName string,
	accountParams map[string]interface{}, queueParams map[string]interface{}) error {
	oldQueueName := GAccount[accountGuid].DefaultQueue

	if oldQueueName != queueName {
		err := NewMns().CreateSimpleQueueByArgs(queueName, accountParams["EndPoint"].(string),
			accountParams["AccessKeyId"].(string), accountParams["AccessKeySecret"].(string))
		if err != nil {
			return fmt.Errorf("AccessKeyId= %s ,AccessKeySecret = %s,EndPoint = %s,|%v", accountParams["AccessKeyId"].(string),
				accountParams["AccessKeySecret"].(string), accountParams["EndPoint"].(string), err)
		}
	}

	return GAccount[accountGuid].Update(accountGuid, queueName, accountParams, queueParams)
}

func (this *Queue) deleteAccount(accountGuid string) error {
	for _, queue := range GAccount[accountGuid].Queue {
		queue.Enable = 0
		queue.IsPubStop = true
		queue.IsSubStop = true
	}

	GAccount[accountGuid].Delete()

	return nil
}
