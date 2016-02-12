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
	"github.com/FactomProject/btcd/wire"
	"github.com/davecgh/go-spew/spew"
)

// ftmMemPool is used as a source of factom transactions
// (CommitChain, RevealChain, CommitEntry, RevealEntry)
type ftmMemPool struct {
	sync.RWMutex
	pool         map[wire.ShaHash]wire.Message
	orphans      map[wire.ShaHash]wire.Message
	blockpool    map[string]wire.Message // to hold the blocks or entries downloaded from peers
	ackpool      []*wire.MsgAcknowledgement
	dirBlockSigs []*wire.MsgDirBlockSig
	lastUpdated  time.Time // last time pool was updated
}

// Add a factom message to the orphan pool
func (mp *ftmMemPool) init_ftmMemPool() error {
	mp.pool = make(map[wire.ShaHash]wire.Message)
	mp.orphans = make(map[wire.ShaHash]wire.Message)
	mp.blockpool = make(map[string]wire.Message)
	mp.ackpool = make([]*wire.MsgAcknowledgement, 0, 200)
	mp.dirBlockSigs = make([]*wire.MsgDirBlockSig, 0, 32)
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

func (mp *ftmMemPool) addAck(ack *wire.MsgAcknowledgement) {
	mp.ackpool[ack.Index] = ack
}

func (mp *ftmMemPool) assembleEomMessages(ack *wire.MsgAcknowledgement) {
	var startIndex int
	if ack.Type == wire.END_MINUTE_1 {
		startIndex = 0
	} else {
		for i, a := range mp.ackpool {
			if a.Type == ack.Type-byte(1) { //match the eom ack prior to ack
				startIndex = i
				break
			}
		}
	}
	procLog.Info("assembleEomMessages, startIndex=%d, for ack=%s", startIndex, spew.Sdump(ack))
	for i := startIndex; i <= int(ack.Index); i++ {
		if mp.ackpool[i] == nil {
			// missing an ACK here
			// todo: request for this ack, panic for now
			panic(fmt.Sprintf("Missing an Ack at index=%d, for ack=%s", i, spew.Sdump(ack)))
		}
	}
	// for a clean state
	for i := startIndex; i <= int(ack.Index); i++ {
		plMgr.AddMyProcessListItem(mp.ackpool[i], mp.ackpool[i].Affirmation, mp.ackpool[i].Type)
	}
	plMgr.AddMyProcessListItem(ack, ack.Affirmation, ack.Type)
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
