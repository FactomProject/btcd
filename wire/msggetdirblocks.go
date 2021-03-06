// Copyright (c) 2013-2015 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"fmt"
	"io"
	//"github.com/FactomProject/FactomCode/util"
)

// MaxBlockLocatorsPerMsg is the maximum number of Directory block locator hashes allowed
// per message.
//const MaxBlockLocatorsPerMsg = 500

// MsgGetDirBlocks implements the Message interface and represents a factom
// getdirblocks message.  It is used to request a list of blocks starting after the
// last known hash in the slice of block locator hashes.  The list is returned
// via an inv message (MsgInv) and is limited by a specific hash to stop at or
// the maximum number of blocks per message, which is currently 500.
//
// Set the HashStop field to the hash at which to stop and use
// AddBlockLocatorHash to build up the list of block locator hashes.
//
// The algorithm for building the block locator hashes should be to add the
// hashes in reverse order until you reach the genesis block.  In order to keep
// the list of locator hashes to a reasonable number of entries, first add the
// most recent 10 block hashes, then double the step each loop iteration to
// exponentially decrease the number of hashes the further away from head and
// closer to the genesis block you get.
type MsgGetDirBlocks struct {
	ProtocolVersion    uint32
	BlockLocatorHashes []*ShaHash
	HashStop           ShaHash
}

// AddBlockLocatorHash adds a new block locator hash to the message.
func (msg *MsgGetDirBlocks) AddBlockLocatorHash(hash *ShaHash) error {
	//util.Trace()
	if len(msg.BlockLocatorHashes)+1 > MaxBlockLocatorsPerMsg {
		str := fmt.Sprintf("too many block locator hashes for message [max %v]",
			MaxBlockLocatorsPerMsg)
		return messageError("MsgGetDirBlocks.AddBlockLocatorHash", str)
	}

	msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, hash)
	return nil
}

// BtcDecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
func (msg *MsgGetDirBlocks) BtcDecode(r io.Reader, pver uint32) error {
	//util.Trace()
	err := readElement(r, &msg.ProtocolVersion)
	if err != nil {
		return err
	}

	// Read num block locator hashes and limit to max.
	count, err := readVarInt(r, pver)
	if err != nil {
		return err
	}
	if count > MaxBlockLocatorsPerMsg {
		str := fmt.Sprintf("too many block locator hashes for message "+
			"[count %v, max %v]", count, MaxBlockLocatorsPerMsg)
		return messageError("MsgGetDirBlocks.BtcDecode", str)
	}

	msg.BlockLocatorHashes = make([]*ShaHash, 0, count)
	for i := uint64(0); i < count; i++ {
		sha := ShaHash{}
		err := readElement(r, &sha)
		if err != nil {
			return err
		}
		msg.AddBlockLocatorHash(&sha)
	}

	err = readElement(r, &msg.HashStop)
	if err != nil {
		return err
	}

	return nil
}

// BtcEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
func (msg *MsgGetDirBlocks) BtcEncode(w io.Writer, pver uint32) error {
	//util.Trace()
	count := len(msg.BlockLocatorHashes)
	if count > MaxBlockLocatorsPerMsg {
		str := fmt.Sprintf("too many block locator hashes for message "+
			"[count %v, max %v]", count, MaxBlockLocatorsPerMsg)
		return messageError("MsgGetDirBlocks.BtcEncode", str)
	}

	err := writeElement(w, msg.ProtocolVersion)
	if err != nil {
		return err
	}

	err = writeVarInt(w, pver, uint64(count))
	if err != nil {
		return err
	}

	for _, hash := range msg.BlockLocatorHashes {
		err = writeElement(w, hash)
		if err != nil {
			return err
		}
	}

	err = writeElement(w, &msg.HashStop)
	if err != nil {
		return err
	}

	return nil
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgGetDirBlocks) Command() string {
	//util.Trace()
	return CmdGetDirBlocks
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgGetDirBlocks) MaxPayloadLength(pver uint32) uint32 {
	// Protocol version 4 bytes + num hashes (varInt) + max block locator
	// hashes + hash stop.
	//util.Trace()
	return 4 + MaxVarIntPayload + (MaxBlockLocatorsPerMsg * HashSize) + HashSize
}

// NewMsgGetDirBlocks returns a new bitcoin getdirblocks message that conforms to the
// Message interface using the passed parameters and defaults for the remaining
// fields.
func NewMsgGetDirBlocks(hashStop *ShaHash) *MsgGetDirBlocks {
	//util.Trace()
	return &MsgGetDirBlocks{
		ProtocolVersion:    ProtocolVersion,
		BlockLocatorHashes: make([]*ShaHash, 0, MaxBlockLocatorsPerMsg),
		HashStop:           *hashStop,
	}
}
