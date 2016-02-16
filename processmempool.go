// Copyright 2015 FactomProject Authors. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package btcd

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/FactomProject/FactomCode/common"
	"github.com/FactomProject/FactomCode/consensus"
	"github.com/FactomProject/btcd/wire"
	"github.com/davecgh/go-spew/spew"
)

// ftmMemPool is used as a source of factom transactions
// (CommitChain, RevealChain, CommitEntry, RevealEntry, Ack, EOM, DirBlockSig)
type ftmMemPool struct {
	sync.RWMutex
	pool             map[wire.ShaHash]wire.Message
	orphans          map[wire.ShaHash]wire.Message
	blockpool        map[string]wire.Message // to hold the blocks or entries downloaded from peers
	ackpool          []*wire.MsgAck
	dirBlockSigs     []*wire.MsgDirBlockSig
	processListItems []*consensus.ProcessListItem
	lastUpdated      time.Time // last time pool was updated
}

// Add a factom message to the orphan pool
func (mp *ftmMemPool) initFtmMemPool() error {
	mp.pool = make(map[wire.ShaHash]wire.Message)
	mp.orphans = make(map[wire.ShaHash]wire.Message)
	mp.blockpool = make(map[string]wire.Message)
	mp.ackpool = make([]*wire.MsgAck, 20000, 20000)
	mp.dirBlockSigs = make([]*wire.MsgDirBlockSig, 0, 32)
	mp.processListItems = make([]*consensus.ProcessListItem, 20000, 20000)
	return nil
}

func (mp *ftmMemPool) addDirBlockSig(dbsig *wire.MsgDirBlockSig) {
	mp.dirBlockSigs = append(mp.dirBlockSigs, dbsig)
}

func (mp *ftmMemPool) resetDirBlockSigPool() {
	mp.dirBlockSigs = make([]*wire.MsgDirBlockSig, 0, 32)
}

func (mp *ftmMemPool) getDirBlockSigPool() []*wire.MsgDirBlockSig {
	return mp.dirBlockSigs
}

// addAck add the ack to ackpool and find it's acknowledged msg.
// then add them to ftmMemPool if available. otherwise return missing acked msg.
func (mp *ftmMemPool) addAck(ack *wire.MsgAck) *wire.Message {
	fmt.Printf("addAck: %+v\n", ack)
	mp.ackpool[ack.Index] = ack
	if ack.Type == wire.ACK_REVEAL_ENTRY || ack.Type == wire.ACK_REVEAL_CHAIN ||
		ack.Type == wire.ACK_COMMIT_CHAIN || ack.Type == wire.ACK_COMMIT_ENTRY {
		msg := mp.pool[*ack.Affirmation]
		if msg == nil {
			// missing msg and request it from the leader ???
			// create a new msg type
			// req := wire.NewMissingMsg(msgHash, Height, Type)
			// return req
		} else {
			pli := &consensus.ProcessListItem{
				Ack:     ack,
				Msg:     msg,
				MsgHash: ack.Affirmation,
			}
			mp.processListItems[ack.Index] = pli
		}
	} else if ack.IsEomAck() {
		pli := &consensus.ProcessListItem{
			Ack:     ack,
			Msg:     ack,
			MsgHash: ack.Affirmation,
		}
		mp.processListItems[ack.Index] = pli
	}
	return nil
}

func (mp *ftmMemPool) getMissingMsgAck(ack *wire.MsgAck) []*wire.MsgAck {
	var i uint32
	var missingAcks []*wire.MsgAck
	for i = 0; i <= ack.Index; i++ {
		if mp.ackpool[i] == nil {
			// missing an ACK here. request for this ack
			a := ack.Clone()
			a.Index = i
			missingAcks = append(missingAcks, a)
			procLog.Infof("Missing an Ack at index=%d", i)
		}
	}
	return missingAcks
}

func (mp *ftmMemPool) assembleEomMessages(ack *wire.MsgAck) error {
	// simply validation
	if ack.Type != wire.END_MINUTE_10 && mp.ackpool[len(mp.ackpool)-1] != ack {
		return fmt.Errorf("the last ack has to be END_MINUTE_10")
	}
	for i := 0; i < len(mp.ackpool); i++ {
		if mp.ackpool[i] == nil {
			// missing an ACK here
			// todo: request for this ack, panic for now
			//panic(fmt.Sprintf("Missing an Ack in ackpool at index=%d, for ack=%s", i, spew.Sdump(ack)))
			fmt.Printf("Missing an Ack in ackpool at index=%d, for ack=%s", i, spew.Sdump(ack))
			continue
		}
		plMgr.AddToLeadersProcessList(mp.ackpool[i], mp.ackpool[i].Affirmation, mp.ackpool[i].Type)
	}
	return nil
}

// Add a factom message to the  Mem pool
func (mp *ftmMemPool) addMsg(msg wire.Message, hash *wire.ShaHash) error {
	if len(mp.pool) > common.MAX_TX_POOL_SIZE {
		return errors.New("Transaction mem pool exceeds the limit.")
	}
	mp.pool[*hash] = msg
	return nil
}

// Add a factom message to the orphan pool
func (mp *ftmMemPool) addOrphanMsg(msg wire.Message, hash *wire.ShaHash) error {
	if len(mp.orphans) > common.MAX_ORPHAN_SIZE {
		return errors.New("Ophan mem pool exceeds the limit.")
	}
	mp.orphans[*hash] = msg
	return nil
}

// Add a factom block message to the  Mem pool
func (mp *ftmMemPool) addBlockMsg(msg wire.Message, hash string) error {
	if len(mp.blockpool) > common.MAX_BLK_POOL_SIZE {
		return errors.New("Block mem pool exceeds the limit. Please restart.")
	}
	mp.Lock()
	mp.blockpool[hash] = msg
	mp.Unlock()
	return nil
}

// Delete a factom block message from the  Mem pool
func (mp *ftmMemPool) deleteBlockMsg(hash string) error {
	if mp.blockpool[hash] != nil {
		mp.Lock()
		delete(fMemPool.blockpool, hash)
		mp.Unlock()
	}
	return nil
}
