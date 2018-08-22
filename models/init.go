package models

import (
	"os"

	"hezhixiong.im/queue/config"
	"hezhixiong.im/queue/models/mns"
	"hezhixiong.im/queue/models/store"
	"hezhixiong.im/share/logging"
	"hezhixiong.im/share/store/redis"
)

var (
	Config     *config.AppConfig
	GAccount   map[string]*mns.Account
	GNameIndex map[string]string
)

func init() {
	Config = config.Default(APP_PID)

	if Config.Settings.Mode < MODE_PUBLISH || Config.Settings.Mode > MODE_PUB_AND_SUB {
		logging.Fatal("Configuration Error With AppSettings.Mode.")
		os.Exit(1)
	}

	// 初始化 Redis 配置
	redis.Init(Config.Redis.Addr, Config.Redis.Db, Config.Redis.Auth, Config.Redis.Timeout)

	// 初始化 Sqlite3 配置
	if Config.Settings.Mode != MODE_SUBSCRIBE {
		if err := store.Init(Config.Sqlite.DriverName, Config.Sqlite.FilePath); err != nil {
			logging.Fatal("Init Sqlite | %v", err)
			os.Exit(1)
		}
	}
}
