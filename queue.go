package main

import (
	_ "hezhixiong.im/queue/controllers"
	. "hezhixiong.im/queue/models"

	"hezhixiong.im/queue/routes"
	"hezhixiong.im/share/app"
	"hezhixiong.im/share/logging"
)

func main() {

	// 项目初始化
	a := app.NewApp(APP_NAME, APP_VERSION)
	a.Cors = Config.Cors.AllowOrigin
	a.DisableGzip = true
	a.LogOn = Config.Log.On
	a.LogAddr = Config.Log.Addr
	a.LogPort = Config.Log.Port
	a.PidName = APP_PID
	a.SessionConfig = Config.Session.Config
	a.SessionOn = Config.Session.On
	a.SessionProviderName = Config.Session.ProviderName
	a.WSPort = Config.Serve.Port

	r := a.Init()

	// 路由注册
	routes.Register(r)

	logging.Error("%v", r.Run(Config.Serve.Port))
}
