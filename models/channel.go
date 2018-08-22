package models

import (
	"sync"
)

// 只支持一读多写
type Channel struct {
	mx        sync.Mutex
	cache     []interface{}
	cache2    []interface{}
	fullNum   int
	FullChan  chan struct{}
	msgNum    int
	isNoticed bool
}

func NewChannel(fullNum int) *Channel {
	return &Channel{
		fullNum:  fullNum,
		cache:    make([]interface{}, 0, fullNum*2),
		cache2:   make([]interface{}, 0, fullNum*2),
		FullChan: make(chan struct{}, 1),
	}
}

func (c *Channel) Add(msgs ...interface{}) {
	var needNotice bool
	c.mx.Lock()
	c.msgNum += len(msgs)
	c.cache = append(c.cache, msgs...)
	needNotice = c.msgNum >= c.fullNum && !c.isNoticed
	if needNotice {
		c.isNoticed = true
	}
	c.mx.Unlock()
	if needNotice {
		c.FullChan <- struct{}{}
	}
}

func (c *Channel) Get() (ret []interface{}) {
	c.mx.Lock()
	ret = c.cache
	c.cache = c.cache2
	c.cache2 = ret
	c.msgNum = 0
	c.cache = c.cache[:0]
	c.isNoticed = false
	c.mx.Unlock()
	return ret
}

func (c *Channel) Len() int {
	c.mx.Lock()
	l := c.msgNum
	c.mx.Unlock()
	return l
}
