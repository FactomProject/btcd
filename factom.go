// Copyright 2015 Factom Foundation
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// Glue code between BTCD code & Factom.

package btcd

import (
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
	inMsgQueue   = make(chan wire.FtmInternalMsg, 100) //incoming message queue for factom application messages
	outMsgQueue  = make(chan wire.FtmInternalMsg, 100) //outgoing message queue for factom application messages
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
				// once it's done block syncing, this is only needed for CLIENT
				// Use broadcast to exclude federate servers
				// todo ???
				dirBlock, _ := msg.(*wire.MsgInt_DirBlock)
				iv := wire.NewInvVect(wire.InvTypeFactomDirBlock, dirBlock.ShaHash)
				s.RelayInventory(iv, nil)
				//msgDirBlock := &wire.MsgDirBlock{DBlk: dirBlock}
				//excludedPeers := make([]*peer, 0, 32)
				//for e := localServer.federateServers.Front(); e != nil; e = e.Next() {
				//excludedPeers = append(excludedPeers, e.Value.(*peer))
				//}
				//s.BroadcastMessage(msgDirBlock, excludedPeers)

			case wire.Message:
				// verify if this wireMsg should be one of MsgEOM, MsgAck,
				// commitEntry/chain, revealEntry/Chain and MsgDirBlockSig
				// need to exclude all peers that are not federate servers
				// todo ???
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
}

func StartBtcd(fcfg *util.FactomdConfig) {

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
