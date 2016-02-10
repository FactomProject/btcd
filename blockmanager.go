// Copyright (c) 2013-2014 Conformal Systems LLC.
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package btcd

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FactomProject/btcd/wire"
	//"github.com/FactomProject/FactomCode/process"
	"github.com/davecgh/go-spew/spew"
)

const (
	chanBufferSize = 50

	// minInFlightBlocks is the minimum number of blocks that should be
	// in the request queue for headers-first mode before requesting
	// more.
	minInFlightBlocks = 10

	// blockDbNamePrefix is the prefix for the block database name.  The
	// database type is appended to this value to form the full block
	// database name.
	blockDbNamePrefix = "blocks"
)

// newPeerMsg signifies a newly connected peer to the block handler.
type newPeerMsg struct {
	peer *peer
}

// blockMsg packages a bitcoin block message and the peer it came from together
// so the block handler has access to that information.
type blockMsg struct {
	//	block *btcutil.Block
	peer *peer
}

// invMsg packages a bitcoin inv message and the peer it came from together
// so the block handler has access to that information.
type invMsg struct {
	inv  *wire.MsgInv
	peer *peer
}

/*
// headersMsg packages a bitcoin headers message and the peer it came from
// together so the block handler has access to that information.
type headersMsg struct {
	headers *wire.MsgHeaders
	peer    *peer
}
*/

// donePeerMsg signifies a newly disconnected peer to the block handler.
type donePeerMsg struct {
	peer *peer
}

// txMsg packages a bitcoin tx message and the peer it came from together
// so the block handler has access to that information.
type txMsg struct {
	//	tx   *btcutil.Tx
	peer *peer
}

// getSyncPeerMsg is a message type to be sent across the message channel for
// retrieving the current sync peer.
type getSyncPeerMsg struct {
	reply chan *peer
}

// checkConnectBlockMsg is a message type to be sent across the message channel
// for requesting chain to check if a block connects to the end of the current
// main chain.
type checkConnectBlockMsg struct {
	//	block *btcutil.Block
	reply chan error
}

// calcNextReqDifficultyResponse is a response sent to the reply channel of a
// calcNextReqDifficultyMsg query.
type calcNextReqDifficultyResponse struct {
	difficulty uint32
	err        error
}

// calcNextReqDifficultyMsg is a message type to be sent across the message
// channel for requesting the required difficulty of the next block.
type calcNextReqDifficultyMsg struct {
	timestamp time.Time
	reply     chan calcNextReqDifficultyResponse
}

// processBlockResponse is a response sent to the reply channel of a
// processBlockMsg.
type processBlockResponse struct {
	isOrphan bool
	err      error
}

// processBlockMsg is a message type to be sent across the message channel
// for requested a block is processed.  Note this call differs from blockMsg
// above in that blockMsg is intended for blocks that came from peers and have
// extra handling whereas this message essentially is just a concurrent safe
// way to call ProcessBlock on the internal block chain instance.
type processBlockMsg struct {
	//	block *btcutil.Block
	//	flags blockchain.BehaviorFlags
	reply chan processBlockResponse
}

// isCurrentMsg is a message type to be sent across the message channel for
// requesting whether or not the block manager believes it is synced with
// the currently connected peers.
type isCurrentMsg struct {
	reply chan bool
}

// headerNode is used as a node in a list of headers that are linked together
// between checkpoints.
type headerNode struct {
	height int64
	sha    *wire.ShaHash
}

// chainState tracks the state of the best chain as blocks are inserted.  This
// is done because btcchain is currently not safe for concurrent access and the
// block manager is typically quite busy processing block and inventory.
// Therefore, requesting this information from chain through the block manager
// would not be anywhere near as efficient as simply updating it as each block
// is inserted and protecting it with a mutex.
type chainState struct {
	sync.Mutex
	newestHash   *wire.ShaHash
	newestHeight int64
}

// Best returns the block hash and height known for the tip of the best known
// chain.
//
// This function is safe for concurrent access.
func (c *chainState) Best() (*wire.ShaHash, int64) {
	c.Lock()
	defer c.Unlock()

	return c.newestHash, c.newestHeight
}

// blockManager provides a concurrency safe block manager for handling all
// incoming blocks.
type blockManager struct {
	server   *server
	started  int32
	shutdown int32
	//	blockChain        *blockchain.BlockChain
	requestedTxns   map[wire.ShaHash]struct{}
	requestedBlocks map[wire.ShaHash]struct{}
	//	progressLogger    *blockProgressLogger
	receivedLogBlocks int64
	receivedLogTx     int64
	processingReqs    bool
	syncPeer          *peer
	msgChan           chan interface{}
	chainState        chainState
	wg                sync.WaitGroup
	quit              chan struct{}
}

// updateChainState updates the chain state associated with the block manager.
// This allows fast access to chain information since btcchain is currently not
// safe for concurrent access and the block manager is typically quite busy
// processing block and inventory.
func (b *blockManager) updateChainState(newestHash *wire.ShaHash, newestHeight int64) {
	b.chainState.Lock()
	defer b.chainState.Unlock()

	b.chainState.newestHash = newestHash
	b.chainState.newestHeight = newestHeight
}

// handleNewPeerMsg deals with new peers that have signalled they may
// be considered as a sync peer (they have already successfully negotiated).  It
// also starts syncing if needed.  It is invoked from the syncHandler goroutine.
func (b *blockManager) handleNewPeerMsg(peers *list.List, p *peer) {
	// Ignore if in the process of shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	bmgrLog.Infof("New valid peer %s (%s)", p, p.userAgent)

	/*
		// Ignore the peer if it's not a sync candidate.
		if !b.isSyncCandidate(p) {
			return
		}

		// Add the peer as a candidate to sync from.
		peers.PushBack(p)

		// Start syncing by choosing the best candidate if needed.
		b.startSync(peers)
	*/

	// Ignore the peer if it's not a sync candidate.
	if !b.isSyncCandidateFactom(p) {
		return
	}

	// Add the peer as a candidate to sync from.
	peers.PushBack(p)

	// Start syncing by choosing the best candidate if needed.
	b.startSyncFactom(peers)
}

// handleDonePeerMsg deals with peers that have signalled they are done.  It
// removes the peer as a candidate for syncing and in the case where it was
// the current sync peer, attempts to select a new best peer to sync from.  It
// is invoked from the syncHandler goroutine.
func (b *blockManager) handleDonePeerMsg(peers *list.List, p *peer) {
	// Remove the peer from the list of candidate peers.
	for e := peers.Front(); e != nil; e = e.Next() {
		if e.Value == p {
			peers.Remove(e)
			break
		}
	}

	bmgrLog.Infof("Lost peer %s", p)

	// Remove requested transactions from the global map so that they will
	// be fetched from elsewhere next time we get an inv.
	for k := range p.requestedTxns {
		delete(b.requestedTxns, k)
	}

	// Remove requested blocks from the global map so that they will be
	// fetched from elsewhere next time we get an inv.
	// TODO(oga) we could possibly here check which peers have these blocks
	// and request them now to speed things up a little.
	for k := range p.requestedBlocks {
		delete(b.requestedBlocks, k)
	}

	// Attempt to find a new peer to sync from if the quitting peer is the
	// sync peer.  Also, reset the headers-first state if in headers-first
	// mode so
	if b.syncPeer != nil && b.syncPeer == p {
		b.syncPeer = nil
		/*
			if b.headersFirstMode {
				// This really shouldn't fail.  We have a fairly
				// unrecoverable database issue if it does.
				newestHash, height, err := db.FetchBlockHeightCache()
				if err != nil {
					bmgrLog.Warnf("Unable to obtain latest "+
						"block information from the database: "+
						"%v", err)
					return
				}
				b.resetHeaderState(newestHash, height)
			}
		*/
		b.startSyncFactom(peers)
	}
}

// current returns true if we believe we are synced with our peers, false if we
// still have blocks to check
func (b *blockManager) current() bool {
	/*
		if !b.blockChain.IsCurrent(b.server.timeSource) {
			return false
		}
	*/
	// if blockChain thinks we are current and we have no syncPeer it
	// is probably right.
	if b.syncPeer == nil {
		return true
	}

	_, height, err := db.FetchBlockHeightCache() //b.server.db.NewestSha()
	// No matter what chain thinks, if we are below the block we are
	// syncing to we are not current.
	// TODO(oga) we can get chain to return the height of each block when we
	// parse an orphan, which would allow us to update the height of peers
	// from what it was at initial handshake.
	if err != nil || height < int64(b.syncPeer.lastBlock) {
		return false
	}

	return true
}

// haveInventory returns whether or not the inventory represented by the passed
// inventory vector is known.  This includes checking all of the various places
// inventory can be when it is in different states such as blocks that are part
// of the main chain, on a side chain, in the orphan pool, and transactions that
// are in the memory pool (either the main pool or orphan pool).
func (b *blockManager) haveInventory(invVect *wire.InvVect) (bool, error) {
	switch invVect.Type {
	case wire.InvTypeBlock:
		panic(errors.New("probably not needed: Factoid1"))
		// Ask chain if the block is known to it in any form (main
		// chain, side chain, or orphan).
		//		return b.blockChain.HaveBlock(&invVect.Hash)

	case wire.InvTypeTx:
		panic(errors.New("needed Factoid1"))

		/*
			// Ask the transaction memory pool if the transaction is known
			// to it in any form (main pool or orphan).
			if b.server.txMemPool.HaveTransaction(&invVect.Hash) {
				return true, nil
			}

			// Check if the transaction exists from the point of view of the
			// end of the main chain.
			return b.server.db.ExistsTxSha(&invVect.Hash)
		*/

	case wire.InvTypeFactomDirBlock:
		// Ask db if the block is known to it in any form (main
		// chain, side chain, or orphan).
		return HaveBlockInDB((&invVect.Hash).ToFactomHash())
	}
	// The requested inventory is is an unsupported type, so just claim
	// it is known to avoid requesting it.
	return true, nil
}

// blockHandler is the main handler for the block manager.  It must be run
// as a goroutine.  It processes block and inv messages in a separate goroutine
// from the peer handlers so the block (MsgBlock) messages are handled by a
// single thread without needing to lock memory data structures.  This is
// important because the block manager controls which blocks are needed and how
// the fetching should proceed.
func (b *blockManager) blockHandler() {
	candidatePeers := list.New()
out:
	for {
		select {
		case m := <-b.msgChan:
			switch msg := m.(type) {
			case *newPeerMsg:
				b.handleNewPeerMsg(candidatePeers, msg.peer)

			case *donePeerMsg:
				b.handleDonePeerMsg(candidatePeers, msg.peer)

			case getSyncPeerMsg:
				msg.reply <- b.syncPeer

				/*
					case checkConnectBlockMsg:
						err := b.blockChain.CheckConnectBlock(msg.block)
						msg.reply <- err

							case calcNextReqDifficultyMsg:
								difficulty, err :=
									b.blockChain.CalcNextRequiredDifficulty(
										msg.timestamp)
								msg.reply <- calcNextReqDifficultyResponse{
									difficulty: difficulty,
									err:        err,
								}
				*/

			case processBlockMsg:
				panic(errors.New("probably not needed: Factoid1"))
				/*
					isOrphan, err := b.blockChain.BC_ProcessBlock(
						msg.block, b.server.timeSource,
						msg.flags)
					if err != nil {
						msg.reply <- processBlockResponse{
							isOrphan: false,
							err:      err,
						}
					}
				*/

				// Query the db for the latest best block since
				// the block that was processed could be on a
				// side chain or have caused a reorg.
				/*
						newestSha, newestHeight, _ := b.server.db.NewestSha()
						b.updateChainState(newestSha, newestHeight)

					msg.reply <- processBlockResponse{
						isOrphan: isOrphan,
						err:      nil,
					}
				*/

			case isCurrentMsg:
				msg.reply <- b.current()
				/*
					case *dirBlockMsg:
						//util.Trace()
						//b.handleDirBlockMsg(msg)
						binary, _ := msg.block.MarshalBinary()
						commonHash := common.Sha(binary)
						blockSha, _ := wire.NewShaHash(commonHash.Bytes)
						delete(msg.peer.requestedBlocks, *blockSha)
						delete(b.requestedBlocks, *blockSha)
						inMsgQueue <- msg
						msg.peer.blockProcessed <- struct{}{}
				*/
			case *dirInvMsg:
				b.handleDirInvMsg(msg)

			default:
				bmgrLog.Warnf("Invalid message type in block "+
					"handler: %T", msg)
				fmt.Printf("before invalid message type: msg=%s\n", spew.Sdump(msg))
				panic(errors.New("invalid message type:"))
			}

		case <-b.quit:
			break out
		}
	}

	b.wg.Done()
	bmgrLog.Trace("Block handler done")
}

// NewPeer informs the block manager of a newly active peer.
func (b *blockManager) NewPeer(p *peer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &newPeerMsg{peer: p}
}

/*
// QueueTx adds the passed transaction message and peer to the block handling
// queue.
func (b *blockManager) QueueTx(tx *btcutil.Tx, p *peer) {
	//	util.Trace()
	// Don't accept more transactions if we're shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		p.txProcessed <- struct{}{}
		return
	}

	b.msgChan <- &txMsg{tx: tx, peer: p}
}

// QueueBlock adds the passed block message and peer to the block handling queue.
func (b *blockManager) QueueBlock(block *btcutil.Block, p *peer) {
	// Don't accept more blocks if we're shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		p.blockProcessed <- struct{}{}
		return
	}

	b.msgChan <- &blockMsg{block: block, peer: p}
}
*/

// QueueInv adds the passed inv message and peer to the block handling queue.
func (b *blockManager) QueueInv(inv *wire.MsgInv, p *peer) {
	//	util.Trace()
	// No channel handling here because peers do not need to block on inv
	// messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &invMsg{inv: inv, peer: p}
}

/*
// QueueHeaders adds the passed headers message and peer to the block handling
// queue.
func (b *blockManager) QueueHeaders(headers *wire.MsgHeaders, p *peer) {
	// No channel handling here because peers do not need to block on
	// headers messages.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &headersMsg{headers: headers, peer: p}
}
*/

// DonePeer informs the blockmanager that a peer has disconnected.
func (b *blockManager) DonePeer(p *peer) {
	// Ignore if we are shutting down.
	if atomic.LoadInt32(&b.shutdown) != 0 {
		return
	}

	b.msgChan <- &donePeerMsg{peer: p}
}

// Start begins the core block handler which processes block and inv messages.
func (b *blockManager) Start() {
	// Already started?
	if atomic.AddInt32(&b.started, 1) != 1 {
		return
	}

	bmgrLog.Trace("Starting block manager")

	b.wg.Add(1)
	go b.blockHandler()
}

// Stop gracefully shuts down the block manager by stopping all asynchronous
// handlers and waiting for them to finish.
func (b *blockManager) Stop() error {
	if atomic.AddInt32(&b.shutdown, 1) != 1 {
		bmgrLog.Warnf("Block manager is already in the process of " +
			"shutting down")
		return nil
	}

	bmgrLog.Infof("Block manager shutting down")
	close(b.quit)
	b.wg.Wait()
	return nil
}

// SyncPeer returns the current sync peer.
func (b *blockManager) SyncPeer() *peer {
	reply := make(chan *peer)
	b.msgChan <- getSyncPeerMsg{reply: reply}
	return <-reply
}

/*
// ProcessBlock makes use of ProcessBlock on an internal instance of a block
// chain.  It is funneled through the block manager since btcchain is not safe
// for concurrent access.
func (b *blockManager) bm_ProcessBlock(block *btcutil.Block, flags blockchain.BehaviorFlags) (bool, error) {
	//util.Trace()
	reply := make(chan processBlockResponse, 1)
	b.msgChan <- processBlockMsg{block: block, flags: flags, reply: reply}
	response := <-reply
	return response.isOrphan, response.err
}
*/

// IsCurrent returns whether or not the block manager believes it is synced with
// the connected peers.
func (b *blockManager) IsCurrent() bool {
	reply := make(chan bool)
	b.msgChan <- isCurrentMsg{reply: reply}
	return <-reply
}

// newBlockManager returns a new bitcoin block manager.
// Use Start to begin processing asynchronous block and inv updates.
func newBlockManager(s *server) (*blockManager, error) {

	/*
		newestHash, height, err := s.db.NewestSha()
		if err != nil {
			return nil, err
		}
	*/

	bm := blockManager{
		server:          s,
		requestedTxns:   make(map[wire.ShaHash]struct{}),
		requestedBlocks: make(map[wire.ShaHash]struct{}),
		//		progressLogger:  newBlockProgressLogger("Processed", bmgrLog),
		msgChan: make(chan interface{}, cfg.MaxPeers*3),
		//		headerList: list.New(),
		quit: make(chan struct{}),
	}

	/*
		//util.Trace(fmt.Sprintf("Hard-Coded GenesisHash= %v\n", activeNetParams.GenesisHash))
		// Initialize the chain state now that the intial block node index has
		// been generated.
		bm.updateChainState(newestHash, height)
	*/

	return &bm, nil
}

// removeRegressionDB removes the existing regression test database if running
// in regression test mode and it already exists.
func removeDB(dbPath string) error {

	// Remove the old database if it already exists.
	fi, err := os.Stat(dbPath)
	if err == nil {
		//		btcdLog.Infof("Removing regression test database from '%s'", dbPath)
		btcdLog.Infof("Removing the database from '%s'", dbPath)
		if fi.IsDir() {
			err := os.RemoveAll(dbPath)
			if err != nil {
				return err
			}
		} else {
			err := os.Remove(dbPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// dbPath returns the path to the block database given a database type.
func blockDbPath(dbType string) string {
	// The database name is based on the database type.
	dbName := blockDbNamePrefix + "_" + dbType
	if dbType == "sqlite" {
		dbName = dbName + ".db"
	}
	dbPath := filepath.Join(cfg.DataDir, dbName)
	return dbPath
}

/*
// warnMultipeDBs shows a warning if multiple block database types are detected.
// This is not a situation most users want.  It is handy for development however
// to support multiple side-by-side databases.
func warnMultipeDBs() {
	// This is intentionally not using the known db types which depend
	// on the database types compiled into the binary since we want to
	// detect legacy db types as well.
	dbTypes := []string{"leveldb", "sqlite"}
	duplicateDbPaths := make([]string, 0, len(dbTypes)-1)
	for _, dbType := range dbTypes {
		if dbType == cfg.DbType {
			continue
		}

		// Store db path as a duplicate db if it exists.
		dbPath := blockDbPath(dbType)
		if fileExists(dbPath) {
			duplicateDbPaths = append(duplicateDbPaths, dbPath)
		}
	}

	// Warn if there are extra databases.
	if len(duplicateDbPaths) > 0 {
		selectedDbPath := blockDbPath(cfg.DbType)
		btcdLog.Warnf("WARNING: There are multiple block chain databases "+
			"using different database types.\nYou probably don't "+
			"want to waste disk space by having more than one.\n"+
			"Your current database is located at [%v].\nThe "+
			"additional database is located at %v", selectedDbPath,
			duplicateDbPaths)
	}
}

// setupBlockDB loads (or creates when needed) the block database taking into
// account the selected database backend.  It also contains additional logic
// such warning the user if there are multiple databases which consume space on
// the file system and ensuring the regression test database is clean when in
// regression test mode.
func setupBlockDB(flag bool) (database.Db, error) {
	//util.Trace("DbType: " + cfg.DbType)

	// The memdb backend does not have a file path associated with it, so
	// handle it uniquely.  We also don't want to worry about the multiple
	// database type warnings when running with the memory database.
	if cfg.DbType == "memdb" {
		btcdLog.Infof("Creating block database in memory.")
		db, err := database.CreateDB(cfg.DbType)
		if err != nil {
			return nil, err
		}
		return db, nil
	}

	//	warnMultipeDBs()

	// The database name is based on the database type.
	dbPath := blockDbPath(cfg.DbType)

	// Remove the database, restart from genesis is requested by the processor.
	if flag {
		removeDB(dbPath)
	}

	btcdLog.Infof("Loading block database from '%s'", dbPath)
	db, err := database.OpenDB(cfg.DbType, dbPath)
	if err != nil {
		// Return the error if it's not because the database
		// doesn't exist.
		if err != database.ErrDbDoesNotExist {
			return nil, err
		}

		// Create the db if it does not exist.
		err = os.MkdirAll(cfg.DataDir, 0700)
		if err != nil {
			return nil, err
		}
		db, err = database.CreateDB(cfg.DbType, dbPath)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

// loadBlockDB opens the block database and returns a handle to it.
func loadBlockDB() (database.Db, error) {
	//util.Trace()

	var removeFlag bool = false

	//util.Trace("FORCE waiting for msg")
	msg := <-outCtlMsgQueue

	msgEom, _ := msg.(*wire.MsgInt_EOM)
	if wire.FORCE_FACTOID_GENESIS_REBUILD == msgEom.EOM_Type {
		//util.Trace("FORCE got it")
		removeFlag = true
	}
	//util.Trace(fmt.Sprintf("FORCE end of waiting for it; height provided= %d", msgEom.NextDBlockHeight))

	db, err := setupBlockDB(removeFlag)
	if err != nil {
		return nil, err
	}

	// Get the latest block height from the database.
	_, height, err := db.NewestSha()
	if err != nil {
		db.Close()
		return nil, err
	}

	// Insert the appropriate genesis block for the bitcoin network being
	// connected to if needed.
	if height == -1 {
		//util.Trace("will insert genesis block")
		genesis := btcutil.NewBlock(activeNetParams.GenesisBlock)
		_, err := db.InsertBlock(genesis)
		if err != nil {
			db.Close()
			//util.Trace(fmt.Sprintf("insert genesis failed: %v", err))
			return nil, err
		}
		btcdLog.Infof("Inserted genesis block %v",
			activeNetParams.GenesisHash)
		height = 0

		gensha, _ := genesis.Sha()

		// verify the inserted genesis block matches the hard-code value
		// Will be taken out once https://github.com/FactomProject/WorkItems/issues/325 is implemented.
		if !chaincfg.MainNetParams.GenesisHash.IsEqual(gensha) {
			panic(errors.New(fmt.Sprintf("Factoid genesis block hash ERROR, during insertion")))
		}

		//		factomIngressBlock_hook(gensha)
	}

	btcdLog.Infof("Block database loaded with block height %d", height)
	return db, nil
}
*/
