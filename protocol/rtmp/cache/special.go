package cache

import (
	"bytes"

	"github.com/NothingYF/livego/protocol/amf"
	"github.com/NothingYF/livego/av"
	"git.scsv.online/go/base/logger"
)

const (
	SetDataFrame string = "@setDataFrame"
	OnMetaData   string = "onMetaData"
)

var setFrameFrame []byte

func init() {
	b := bytes.NewBuffer(nil)
	encoder := &amf.Encoder{}
	if _, err := encoder.Encode(b, SetDataFrame, amf.AMF0); err != nil {
		logger.Error(err.Error())
	}
	setFrameFrame = b.Bytes()
}

type SpecialCache struct {
	full bool
	p    *av.Packet
}

func NewSpecialCache() *SpecialCache {
	return &SpecialCache{}
}

func (specialCache *SpecialCache) Write(p *av.Packet) {
	specialCache.p = p
	specialCache.full = true
}

func (specialCache *SpecialCache) Send(w av.WriteCloser) error {
	if !specialCache.full {
		return nil
	}
	return w.Write(specialCache.p)
}
