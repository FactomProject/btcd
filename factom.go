// Copyright 2015 Factom Foundation
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// Glue code between BTCD code & Factom.

package btcd

import (
	"errors"
	"fmt"
	"os"

	"github.com/FactomProject/FactomCode/common"
	cp "github.com/FactomProject/FactomCode/controlpanel"
	"github.com/FactomProject/FactomCode/database"
	"github.com/FactomProject/FactomCode/util"
	"github.com/FactomProject/btcd/wire"
)

var _ = fmt.Printf

var (
	localServer  *server
	db           database.Db // database
	factomConfig *util.FactomdConfig
	inMsgQueue   chan wire.FtmInternalMsg //incoming message queue for factom application messages
	outMsgQueue  chan wire.FtmInternalMsg //outgoing message queue for factom application messages

	inCtlMsgQueue  chan wire.FtmInternalMsg //incoming message queue for factom control messages
	outCtlMsgQueue chan wire.FtmInternalMsg //outgoing message queue for factom control messages
)

// start up Factom queue(s) managers/processors
// this is to be called within the btcd's main code
func factomForkInit(s *server) {
	// tweak some config options
	cfg.DisableCheckpoints = true

	localServer = s // local copy of our server pointer

	// Write outgoing factom messages into P2P network
	go func() {
		for msg := range outMsgQueue {
			switch msg.(type) {
			case *wire.MsgInt_DirBlock:
				dirBlock, _ := msg.(*wire.MsgInt_DirBlock)
				iv := wire.NewInvVect(wire.InvTypeFactomDirBlock, dirBlock.ShaHash)
				s.RelayInventory(iv, nil)

			case wire.Message:
				// verify if this wireMsg should be one of MsgEOM, MsgAck, commitEntry/chain, revealEntry/Chain
				wireMsg, _ := msg.(wire.Message)
				s.BroadcastMessage(wireMsg)
				/*
					if ClientOnly {
						//fmt.Println("broadcasting from client.")
						s.BroadcastMessage(wireMsg)
					} else {
						if _, ok := msg.(*wire.MsgAcknowledgement); ok {
							//fmt.Println("broadcasting from server.")
							s.BroadcastMessage(wireMsg)
						}
					}*/

			default:
				panic(fmt.Sprintf("bad outMsgQueue message received: %v", msg))
			}
			/*      peerInfoResults := server.PeerInfo()
			        for peerInfo := range peerInfoResults{
			          fmt.Printf("PeerInfo:%+v", peerInfo)

			        }*/
		}
	}()

	go func() {
		for msg := range outCtlMsgQueue {

			fmt.Printf("in range outCtlMsgQueue, msg:%+v\n", msg)

			msgEom, _ := msg.(*wire.MsgInt_EOM)

			//			switch msgEom.Command() {
			switch msg.Command() {

			case wire.CmdInt_EOM:

				switch msgEom.EOM_Type {

				case wire.END_MINUTE_10:
					panic(errors.New("unhandled END_MINUTE_10"))

				default:
					panic(errors.New("unhandled EOM type"))
				}

			default:
				panic(errors.New("unhandled CmdInt_EOM"))
			}
		}
	}()
}

func Start_btcd(fcfg *util.FactomdConfig) {

	factomConfig = fcfg
	if common.SERVER_NODE != fcfg.App.NodeMode {
		ClientOnly = true
	}

	if ClientOnly {
		cp.CP.AddUpdate(
			"FactomMode", // tag
			"system",     // Category
			"Factom Mode: Full Node (Client)", // Title
			"", // Message
			0)
		fmt.Println("\n\n>>>>>>>>>>>>>>>>>  CLIENT MODE <<<<<<<<<<<<<<<<<<<<<<<\n\n")
	} else {
		cp.CP.AddUpdate(
			"FactomMode",                    // tag
			"system",                        // Category
			"Factom Mode: Federated Server", // Title
			"", // Message
			0)
		fmt.Println("\n\n>>>>>>>>>>>>>>>>>  SERVER MODE <<<<<<<<<<<<<<<<<<<<<<<\n\n")
	}

	// Work around defer not working after os.Exit()
	if err := btcdMain(nil); err != nil {
		os.Exit(1)
	}
}
