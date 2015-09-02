// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package wire

import (
	"io"

	"github.com/FactomProject/FactomCode/common"
)

// MsgEntry implements the Message interface and represents a factom
// Entry message.  It is used by client to reveal the entry.
type MsgEntry struct {
	Entry *common.Entry
}

// BtcEncode encodes the receiver to w using the bitcoin protocol encoding.
// This is part of the Message interface implementation.
func (msg *MsgEntry) BtcEncode(w io.Writer, pver uint32) error {
	bytes, err := msg.Entry.MarshalBinary()
	if err != nil {
		return err
	}

	err = writeVarBytes(w, pver, bytes)
	if err != nil {
		return err
	}

	return nil
}

// BtcDecode decodes r using the bitcoin protocol encoding into the receiver.
// This is part of the Message interface implementation.
func (msg *MsgEntry) BtcDecode(r io.Reader, pver uint32) error {
	//Entry
	bytes, err := readVarBytes(r, pver, uint32(MaxAppMsgPayload), CmdEntry)
	if err != nil {
		return err
	}

	msg.Entry = new(common.Entry)
	err = msg.Entry.UnmarshalBinary(bytes)
	if err != nil {
		return err
	}

	return nil
}

// Command returns the protocol command string for the message.  This is part
// of the Message interface implementation.
func (msg *MsgEntry) Command() string {
	return CmdEntry
}

// MaxPayloadLength returns the maximum length the payload can be for the
// receiver.  This is part of the Message interface implementation.
func (msg *MsgEntry) MaxPayloadLength(pver uint32) uint32 {
	return MaxAppMsgPayload
}

// NewMsgEntry returns a new bitcoin inv message that conforms to the Message
// interface.  See MsgInv for details.
func NewMsgEntry() *MsgEntry {
	return &MsgEntry{}
}

