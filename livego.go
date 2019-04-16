package main

import (
	"flag"
	"github.com/NothingYF/livego/configure"
	"github.com/NothingYF/livego/protocol/hls"
	"github.com/NothingYF/livego/protocol/httpflv"
	"github.com/NothingYF/livego/protocol/httpopera"
	"github.com/NothingYF/livego/protocol/rtmp"
	"log"
	"net"
	"time"
	"git.scsv.online/go/base/logger"
)

var (
	version        = "master"
	rtmpAddr       = flag.String("rtmp-addr", ":1935", "RTMP server listen address")
	httpFlvAddr    = flag.String("httpflv-addr", ":7001", "HTTP-FLV server listen address")
	hlsAddr        = flag.String("hls-addr", ":7002", "HLS server listen address")
	operaAddr      = flag.String("manage-addr", ":8090", "HTTP manage interface server listen address")
	configfilename = flag.String("cfgfile", "livego.cfg", "live configure filename")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime | log.Ldate)
	flag.Parse()
}

func startHls() *hls.Server {
	hlsListen, err := net.Listen("tcp", *hlsAddr)
	if err != nil {
		logger.Error(err.Error())
	}

	hlsServer := hls.NewServer()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Println("HLS server panic: ", r)
			}
		}()
		logger.Println("HLS listen On", *hlsAddr)
		hlsServer.Serve(hlsListen)
	}()
	return hlsServer
}

func startRtmp(stream *rtmp.RtmpStream, hlsServer *hls.Server) {
	rtmpListen, err := net.Listen("tcp", *rtmpAddr)
	if err != nil {
		logger.Error(err.Error())
	}

	var rtmpServer *rtmp.Server

	if hlsServer == nil {
		rtmpServer = rtmp.NewRtmpServer(stream, nil)
		logger.Debug("hls server disable....")
	} else {
		rtmpServer = rtmp.NewRtmpServer(stream, hlsServer)
		logger.Debug("hls server enable....")
	}

	defer func() {
		if r := recover(); r != nil {
			logger.Println("RTMP server panic: ", r)
		}
	}()
	logger.Println("RTMP Listen On", *rtmpAddr)
	rtmpServer.Serve(rtmpListen)
}

func startHTTPFlv(stream *rtmp.RtmpStream) {
	flvListen, err := net.Listen("tcp", *httpFlvAddr)
	if err != nil {
		logger.Error(err.Error())
	}

	hdlServer := httpflv.NewServer(stream)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Println("HTTP-FLV server panic: ", r)
			}
		}()
		logger.Println("HTTP-FLV listen On", *httpFlvAddr)
		hdlServer.Serve(flvListen)
	}()
}

func startHTTPOpera(stream *rtmp.RtmpStream) {
	if *operaAddr != "" {
		opListen, err := net.Listen("tcp", *operaAddr)
		if err != nil {
			logger.Error(err.Error())
		}
		opServer := httpopera.NewServer(stream, *rtmpAddr)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Println("HTTP-Operation server panic: ", r)
				}
			}()
			logger.Println("HTTP-Operation listen On", *operaAddr)
			opServer.Serve(opListen)
		}()
	}
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			logger.Println("livego panic: ", r)
			time.Sleep(1 * time.Second)
		}
	}()
	logger.Println("start livego, version", version)
	err := configure.LoadConfig(*configfilename)
	if err != nil {
		return
	}

	stream := rtmp.NewRtmpStream()
	hlsServer := startHls()
	startHTTPFlv(stream)
	startHTTPOpera(stream)

	startRtmp(stream, hlsServer)
	//startRtmp(stream, nil)
}
