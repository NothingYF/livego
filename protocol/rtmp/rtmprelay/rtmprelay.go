package rtmprelay

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/NothingYF/livego/protocol/amf"
	"github.com/NothingYF/livego/protocol/rtmp/core"
	"io"
	"git.scsv.online/go/base/logger"
)

var (
	STOP_CTRL = "RTMPRELAY_STOP"
)

type RtmpRelay struct {
	PlayUrl              string
	PublishUrl           string
	cs_chan              chan core.ChunkStream
	sndctrl_chan         chan string
	connectPlayClient    *core.ConnClient
	connectPublishClient *core.ConnClient
	startflag            bool
}

func NewRtmpRelay(playurl *string, publishurl *string) *RtmpRelay {
	return &RtmpRelay{
		PlayUrl:              *playurl,
		PublishUrl:           *publishurl,
		cs_chan:              make(chan core.ChunkStream, 500),
		sndctrl_chan:         make(chan string),
		connectPlayClient:    nil,
		connectPublishClient: nil,
		startflag:            false,
	}
}

func (self *RtmpRelay) rcvPlayChunkStream() {
	logger.Println("rcvPlayRtmpMediaPacket connectClient.Read...")
	for {
		var rc core.ChunkStream

		if self.startflag == false {
			self.connectPlayClient.Close(nil)
			logger.Debug("rcvPlayChunkStream close: playurl=%s, publishurl=%s", self.PlayUrl, self.PublishUrl)
			break
		}
		err := self.connectPlayClient.Read(&rc)

		if err != nil && err == io.EOF {
			break
		}
		//logger.Debug("connectPlayClient.Read return rc.TypeID=%v length=%d, err=%v", rc.TypeID, len(rc.Data), err)
		switch rc.TypeID {
		case 20, 17:
			r := bytes.NewReader(rc.Data)
			vs, err := self.connectPlayClient.DecodeBatch(r, amf.AMF0)

			logger.Debug("rcvPlayRtmpMediaPacket: vs=%v, err=%v", vs, err)
		case 18:
			logger.Debug("rcvPlayRtmpMediaPacket: metadata....")
		case 8, 9:
			self.cs_chan <- rc
		}
	}
}

func (self *RtmpRelay) sendPublishChunkStream() {
	for {
		select {
		case rc := <-self.cs_chan:
			//logger.Debug("sendPublishChunkStream: rc.TypeID=%v length=%d", rc.TypeID, len(rc.Data))
			self.connectPublishClient.Write(rc)
		case ctrlcmd := <-self.sndctrl_chan:
			if ctrlcmd == STOP_CTRL {
				self.connectPublishClient.Close(nil)
				logger.Debug("sendPublishChunkStream close: playurl=%s, publishurl=%s", self.PlayUrl, self.PublishUrl)
				break
			}
		}
	}
}

func (self *RtmpRelay) Start() error {
	if self.startflag {
		err := errors.New(fmt.Sprintf("The rtmprelay already started, playurl=%s, publishurl=%s", self.PlayUrl, self.PublishUrl))
		return err
	}

	self.connectPlayClient = core.NewConnClient()
	self.connectPublishClient = core.NewConnClient()

	logger.Debug("play server addr:%v starting....", self.PlayUrl)
	err := self.connectPlayClient.Start(self.PlayUrl, "play")
	if err != nil {
		logger.Debug("connectPlayClient.Start url=%v error", self.PlayUrl)
		return err
	}

	logger.Debug("publish server addr:%v starting....", self.PublishUrl)
	err = self.connectPublishClient.Start(self.PublishUrl, "publish")
	if err != nil {
		logger.Debug("connectPublishClient.Start url=%v error", self.PublishUrl)
		self.connectPlayClient.Close(nil)
		return err
	}

	self.startflag = true
	go self.rcvPlayChunkStream()
	go self.sendPublishChunkStream()

	return nil
}

func (self *RtmpRelay) Stop() {
	if !self.startflag {
		logger.Debug("The rtmprelay already stoped, playurl=%s, publishurl=%s", self.PlayUrl, self.PublishUrl)
		return
	}

	self.startflag = false
	self.sndctrl_chan <- STOP_CTRL

}
