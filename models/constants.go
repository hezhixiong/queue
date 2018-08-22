package models

const (
	APP_NAME    = "Faccwork_Queue"
	APP_VERSION = "1.4.8.fec1"
	APP_PID     = "hezhixiong_queue"
)

// 消息服务启动模式
const (
	MODE_PUBLISH     = 1
	MODE_SUBSCRIBE   = 2
	MODE_PUB_AND_SUB = 3
)

const (
	DEF_PUB_MAX_MSG_NUMS          = 16
	DEF_PUB_MAX_DELAY_SEC         = 16
	DEF_PUB_ERR_WAIT_SEC          = 5
	DEF_PUB_ERR_MAX_WAIT_SEC      = 600
	DEF_SUB_MAX_MSG_NUMS          = 16
	DEF_SUB_POLLING_CYCLE_SEC     = 16
	DEF_CALLBACK_ERR_WAIT_SEC     = 5
	DEF_CALLBACK_ERR_MAX_WAIT_SEC = 600
)

const (
	TIME_STAMP_TOLERANCE = 15 // 时间戳误差，单位：分钟
)

// Mail Template
// --------------------------------------------------------------------------------

const (
	TEMPLATE_MAIL_MNS_WARNING = "template/email/mns_warning.tpl"
)

const (
	EXIT_BRPOPLPUSH_MSG = "For Exit BRPOPLPUSH"
)
