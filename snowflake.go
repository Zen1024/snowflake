package snowflake

import (
	"fmt"
	"sync"
	"time"
)

const (
	defWorkerIdBits = 10
	defSeqIdBits    = 12
	defTsBits       = 41
)

type SnowFlake struct {
	sync.Mutex
	WorkerIdBits int
	SeqIdBits    int
	TsBits       int
	last_ts      int64
	last_seq     int64
	WorkerId     int64
	maxWorkerId  int64
	maxSeqId     int64
}

func nowMilli() int64 {
	now := time.Now().UnixNano()
	return now / 1e6
}

func defSnowFlake(workerId int64) *SnowFlake {
	return &SnowFlake{
		WorkerIdBits: defWorkerIdBits,
		SeqIdBits:    defSeqIdBits,
		TsBits:       defTsBits,
		last_ts:      nowMilli(),
		WorkerId:     workerId,
		last_seq:     int64(0),
		maxWorkerId:  int64(1 << uint(defWorkerIdBits)),
		maxSeqId:     int64(1 << uint(defSeqIdBits)),
	}
}

//根据默认参数配置创建
func DefaultSnowFlake(workerId int64) *SnowFlake {
	return defSnowFlake(workerId)
}

func NewSnoWflake(workeridBit, seqidBit, tsidBit int, workerId int64) *SnowFlake {
	if workeridBit <= 0 ||
		seqidBit <= 0 ||
		tsidBit <= 0 ||
		(workeridBit+seqidBit+tsidBit != 63) ||
		workerId > int64(1<<uint(workeridBit)) {
		re := defSnowFlake(workerId)
		re.WorkerId = workerId
		return re
	}

	return &SnowFlake{
		WorkerIdBits: workeridBit,
		WorkerId:     workerId,
		SeqIdBits:    seqidBit,
		TsBits:       tsidBit,
		last_ts:      nowMilli(),
		last_seq:     int64(0),
		maxWorkerId:  int64(1 << uint(workeridBit)),
		maxSeqId:     int64(1 << uint(seqidBit)),
	}
}

func (s *SnowFlake) GenId() (int64, error) {
	s.Lock()
	defer s.Unlock()
	ts_milli := nowMilli()
	if ts_milli < s.last_ts {
		return -1, fmt.Errorf("invalid last_ts:%d", s.last_ts)
	}
	if ts_milli == s.last_ts {
		if s.last_seq == s.maxSeqId {
			s.updateTs()
		} else {
			s.last_seq++
		}
	} else {
		s.last_ts = ts_milli
		s.last_seq = int64(0)
	}
	return s.genId(), nil
}

func (s *SnowFlake) genId() int64 {
	ts := s.last_ts
	workerId := s.WorkerId
	seqid := s.last_seq
	ts_shift := uint(s.WorkerIdBits + s.SeqIdBits)
	wk_shift := uint(s.SeqIdBits)
	return ts<<ts_shift | workerId<<wk_shift | seqid
}

func (s *SnowFlake) updateTs() {
	s.last_ts += 1
	s.last_seq = int64(0)
}
