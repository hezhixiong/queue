package controllers

import (
	"os"

	. "hezhixiong.im/queue/models"

	m "hezhixiong.im/queue/controllers/mns"
	"hezhixiong.im/queue/models/mns"
	"hezhixiong.im/share/logging"
)

func init() {

	// 运行模式
	var runMode string
	switch Config.Settings.Mode {
	case MODE_PUBLISH:
		runMode = "Only_Publish"
	case MODE_SUBSCRIBE:
		runMode = "Only_Subscribe"
	case MODE_PUB_AND_SUB:
		runMode = "Publish_And_Subscribe"
	default:
	}
	logging.Debug("hezhixiong_queue run mode: %v", runMode)

	// 初始化全局变量 GAccount
	accs, err := mns.NewAccount().GetAccounts()
	if err != nil {
		logging.Error("Get Accounts | %v", err)
		os.Exit(1)
		return
	}

	GAccount = make(map[string]*mns.Account, len(accs)+1)
	GNameIndex = make(map[string]string, len(accs)+1)

	for _, accountGuid := range accs {
		a := mns.NewAccount()
		if err := a.GetSingle(accountGuid); err == nil {
			GAccount[accountGuid] = a
			GNameIndex[a.AccountName] = accountGuid
			logging.Debug("Initializtion Success | %v", a.AccountGuid)
		}
	}

	alimns := m.NewMns()
	for _, account := range GAccount {
		for _, queue := range account.Queue {
			if err := alimns.CreateSimpleQueue(queue.QueueName, account.Client); err != nil {
				logging.Error("Create Simple Queue | [%v] -> [%v] | %v", account.AccountGuid, queue.QueueName, err)
				os.Exit(1)
				return
			}
			logging.Debug("Create Simple Queue Success | [%v] -> [%v]", account.AccountGuid, queue.QueueName)
		}
	}

	alimns.PublishAll()
	alimns.SubscribeAll()
}
