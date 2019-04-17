package rtmp

import (
	"errors"
	"github.com/NothingYF/livego/av"
	"github.com/NothingYF/livego/protocol/rtmp/cache"
	"github.com/NothingYF/livego/protocol/rtmp/rtmprelay"
	"github.com/orcaman/concurrent-map"
	"strings"
	"time"
	"git.scsv.online/go/base/logger"
	"git.scsv.online/go/base/util"
)

var (
	EmptyID = ""
)

type RtmpStream struct {
	streams cmap.ConcurrentMap //key
}

func NewRtmpStream() *RtmpStream {
	ret := &RtmpStream{
		streams: cmap.New(),
	}
	go ret.CheckAlive()
	return ret
}

func (rs *RtmpStream) HandleReader(r av.ReadCloser) {
	info := r.Info()
	logger.Debug("HandleReader: info[%v]", info)

	var stream *Stream
	i, ok := rs.streams.Get(info.Key)
	if stream, ok = i.(*Stream); ok {
		stream.TransStop()
		id := stream.ID()
		if id != EmptyID && id != info.UID {
			ns := NewStream()
			stream.Copy(ns)
			stream = ns
			rs.streams.Set(info.Key, ns)
		}
	} else {
		stream = NewStream()
		rs.streams.Set(info.Key, stream)
		stream.info = info
	}

	stream.AddReader(r)
}

func (rs *RtmpStream) HandleWriter(w av.WriteCloser) {
	info := w.Info()
	logger.Debug("HandleWriter: info[%v]", info)

	var s *Stream
	ok := rs.streams.Has(info.Key)
	if !ok {
		s = NewStream()
		rs.streams.Set(info.Key, s)
		s.info = info
	} else {
		item, ok := rs.streams.Get(info.Key)
		if ok {
			s = item.(*Stream)
			s.AddWriter(w)
		}
	}
}

func (rs *RtmpStream) GetStreams() cmap.ConcurrentMap {
	return rs.streams
}

func (rs *RtmpStream) CheckAlive() {
	for {
		<-time.After(5 * time.Second)
		for item := range rs.streams.IterBuffered() {
			v := item.Val.(*Stream)
			if v.CheckAlive() == 0 {
				rs.streams.Remove(item.Key)
			}
		}
	}
}

type Stream struct {
	isStart bool
	cache   *cache.Cache
	r       av.ReadCloser
	ws      cmap.ConcurrentMap
	info    av.Info
}

type PackWriterCloser struct {
	init bool
	w    av.WriteCloser
}

func (p *PackWriterCloser) GetWriter() av.WriteCloser {
	return p.w
}

func NewStream() *Stream {
	return &Stream{
		cache: cache.NewCache(),
		ws:    cmap.New(),
	}
}

func (s *Stream) ID() string {
	if s.r != nil {
		return s.r.Info().UID
	}
	return EmptyID
}

func (s *Stream) GetReader() av.ReadCloser {
	return s.r
}

func (s *Stream) GetWs() cmap.ConcurrentMap {
	return s.ws
}

func (s *Stream) Copy(dst *Stream) {
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		s.ws.Remove(item.Key)
		v.w.CalcBaseTimestamp()
		dst.AddWriter(v.w)
	}
}

func (s *Stream) AddReader(r av.ReadCloser) {
	s.r = r
	go s.TransStart()
}

func (s *Stream) AddWriter(w av.WriteCloser) {
	info := w.Info()
	pw := &PackWriterCloser{w: w}
	s.ws.Set(info.UID, pw)
}

/*检测本application下是否配置static_push,
如果配置, 启动push远端的连接*/
func (s *Stream) StartStaticPush() {
	key := s.info.Key

	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return
	}

	streamname := key[index+1:]
	appname := dscr[0]

	logger.Debug("StartStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		logger.Debug("StartStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		logger.Debug("StartStaticPush: static pushurl=%s", pushurl)

		staticpushObj := rtmprelay.GetAndCreateStaticPushObject(pushurl)
		if staticpushObj != nil {
			if err := staticpushObj.Start(); err != nil {
				logger.Debug("StartStaticPush: staticpushObj.Start %s error=%v", pushurl, err)
			} else {
				logger.Debug("StartStaticPush: staticpushObj.Start %s ok", pushurl)
			}
		} else {
			logger.Debug("StartStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *Stream) StopStaticPush() {
	key := s.info.Key

	logger.Debug("StopStaticPush......%s", key)
	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return
	}

	streamname := key[index+1:]
	appname := dscr[0]

	logger.Debug("StopStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		logger.Debug("StopStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		logger.Debug("StopStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.Stop()
			rtmprelay.ReleaseStaticPushObject(pushurl)
			logger.Debug("StopStaticPush: staticpushObj.Stop %s ", pushurl)
		} else {
			logger.Debug("StopStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *Stream) IsSendStaticPush() bool {
	key := s.info.Key

	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return false
	}

	appname := dscr[0]

	//logger.Debug("SendStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		//logger.Debug("SendStaticPush: GetStaticPushList error=%v", err)
		return false
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return false
	}

	streamname := key[index+1:]

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		//logger.Debug("SendStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			return true
			//staticpushObj.WriteAvPacket(&packet)
			//logger.Debug("SendStaticPush: WriteAvPacket %s ", pushurl)
		} else {
			logger.Debug("SendStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
	return false
}

func (s *Stream) SendStaticPush(packet av.Packet) {
	key := s.info.Key

	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return
	}

	streamname := key[index+1:]
	appname := dscr[0]

	//logger.Debug("SendStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		//logger.Debug("SendStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		//logger.Debug("SendStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.WriteAvPacket(&packet)
			//logger.Debug("SendStaticPush: WriteAvPacket %s ", pushurl)
		} else {
			logger.Debug("SendStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *Stream) TransStart() {
	defer util.PanicTrace(false)

	s.isStart = true
	var p av.Packet

	logger.Debug("TransStart:%v", s.info)

	s.StartStaticPush()

	for {
		if !s.isStart {
			s.closeInter()
			return
		}
		err := s.r.Read(&p)
		if err != nil {
			s.closeInter()
			s.isStart = false
			return
		}

		if s.IsSendStaticPush() {
			s.SendStaticPush(p)
		}

		s.cache.Write(p)

		for item := range s.ws.IterBuffered() {
			v := item.Val.(*PackWriterCloser)
			if !v.init {
				//logger.Debug("cache.send: %v", v.w.Info())
				if err = s.cache.Send(v.w); err != nil {
					logger.Debug("[%s] send cache packet error: %v, remove", v.w.Info(), err)
					s.ws.Remove(item.Key)
					continue
				}
				v.init = true
			} else {
				new_packet := p
				//writeType := reflect.TypeOf(v.w)
				//logger.Debug("w.Write: type=%v, %v", writeType, v.w.Info())
				if err = v.w.Write(&new_packet); err != nil {
					logger.Debug("[%s] write packet error: %v, remove", v.w.Info(), err)
					s.ws.Remove(item.Key)
				}
			}
		}
	}
}

func (s *Stream) TransStop() {
	logger.Debug("TransStop: %s", s.info.Key)

	if s.isStart && s.r != nil {
		s.r.Close(errors.New("stop old"))
	}

	s.isStart = false
}

func (s *Stream) CheckAlive() (n int) {
	if s.r != nil && s.isStart {
		if s.r.Alive() {
			n++
		} else {
			s.r.Close(errors.New("read timeout"))
		}
	}
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		if v.w != nil {
			if !v.w.Alive() && s.isStart {
				s.ws.Remove(item.Key)
				v.w.Close(errors.New("write timeout"))
				continue
			}
			n++
		}

	}
	return
}

func (s *Stream) closeInter() {
	if s.r != nil {
		s.StopStaticPush()
		logger.Debug("[%v] publisher closed", s.r.Info())
	}

	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		if v.w != nil {
			if v.w.Info().IsInterval() {
				v.w.Close(errors.New("closed"))
				s.ws.Remove(item.Key)
				logger.Debug("[%v] player closed and remove\n", v.w.Info())
			}
		}

	}
}
