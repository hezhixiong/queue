package config

import (
	"hezhixiong.im/share/lib"
)

var config *AppConfig

type AppConfig struct {
	Settings AppSettings    `xml:"appSettings"`
	Email    EmailSetting   `xml:"emailSetting"`
	Cors     CorsSetting    `xml:"cors"`
	Log      LogServer      `xml:"logServer"`
	Serve    ListenAndServe `xml:"listenAndServe"`
	Session  SessionSetting `xml:"session"`
	Redis    RedisStore     `xml:"redisStore"`
	Sqlite   SqliteStore    `xml:"sqliteStore"`
}

// Settings
type AppSettings struct {
	AllowOrigin  string     `xml:"allowOrigin"`
	EncryFactor  string     `xml:"encryFactor"`
	Environment  string     `xml:"environment"`
	Listen       string     `xml:"listen"`
	Projects     []Projects `xml:"projects"`
	Mode         int        `xml:"mode"`
	SupportEmail string     `xml:"supportEmail"`
	IsDiscard    int        `xml:"isDiscard"`
}
type Projects struct {
	AppId      string `xml:"appId"`
	ConfigFile string `xml:"configFile"`
}

// Email
type EmailSetting struct {
	Addr     string `xml:"addr"`
	Password string `xml:"password"`
	Server   string `xml:"server"`
	Port     string `xml:"port"`
}

// Cors
type CorsSetting struct {
	AllowOrigin []string `xml:"allowOrigin"`
}

// Log
type LogServer struct {
	On   bool   `xml:"on"`
	Addr string `xml:"addr"`
	Port string `xml:"port"`
}

// Serve
type ListenAndServe struct {
	Port    string `xml:"port"`
	LogPort string `xml:"logport"`
}

// Session
type SessionSetting struct {
	On           bool   `xml:"on"`
	ProviderName string `xml:"providerName"`
	Config       string `xml:"config"`
}

// Redis
type RedisStore struct {
	Addr    string `xml:"addr"`
	Auth    string `xml:"auth"`
	Db      string `xml:"db"`
	Timeout int    `xml:"timeout"`
}

// Sqlite
type SqliteStore struct {
	DriverName string `xml:"driverName"`
	FilePath   string `xml:"filePath"`
}

// ------------------------------------------------------------------------

func Default(appID string) *AppConfig {
	if config == nil {
		var cfg AppConfig
		lib.LoadConfig(appID, &cfg)
		config = &cfg
	}
	return config
}
