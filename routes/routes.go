package routes

import (
	"hezhixiong.im/queue/controllers/mns"

	"github.com/gin-gonic/gin"
)

func Register(engine *gin.Engine) {
	rg := engine.Group("/api")

	// --------------------------------------------------------------------------------

	// 消息推送服务
	rg.POST("/queue/publish", mns.NewQueue().SendMessage)

	// 微信代理·消息推送服务
	rg.POST("/smartchat/wechat/exchange", mns.NewQueue().Exchange)

	// 微信代理·接口式 添加·修改企业组织
	rg.PUT("/smartchat/wechat/organize", mns.NewQueue().PutOrganize)

	// 微信代理·接口式 删除企业组织
	rg.DELETE("/smartchat/wechat/organize", mns.NewQueue().DelOrganize)
}
