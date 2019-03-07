package discovery

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/lntest"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/ticker"
)

// randPeer creates a random peer.
func randPeer(t *testing.T, quit chan struct{}) *mockPeer {
	t.Helper()

	return &mockPeer{
		pk:       randPubKey(t),
		sentMsgs: make(chan lnwire.Message),
		quit:     quit,
	}
}

// newTestSyncManager creates a new test syncManager using mock implementations
// of its dependencies.
func newTestSyncManager(numActiveSyncers int) *syncManager {
	hID := lnwire.ShortChannelID{BlockHeight: latestKnownHeight}
	return newSyncManager(&syncManagerCfg{
		ChanSeries:                newMockChannelGraphTimeSeries(hID),
		RotateTicker:              ticker.NewForce(DefaultSyncerRotationInterval),
		FullSyncTicker:            ticker.NewForce(DefaultFullSyncInterval),
		ActiveSyncerTimeoutTicker: ticker.NewForce(DefaultActiveSyncerTimeout),
		NumActiveSyncers:          numActiveSyncers,
	})
}

// TestSyncManagerNumActiveSyncers ensures that we are unable to have more than
// NumActiveSyncers active syncers.
func TestSyncManagerNumActiveSyncers(t *testing.T) {
	t.Parallel()

	// We'll start by creating our test sync manager which will hold up to
	// 3 active syncers.
	const numActiveSyncers = 3
	const numSyncers = numActiveSyncers + 1

	syncMgr := newTestSyncManager(numActiveSyncers)
	syncMgr.Start()
	defer syncMgr.Stop()

	// We'll go ahead and create our syncers. We'll gather the ones which
	// should be active and passive to check them later on.
	activeSyncPeers := make([]*mockPeer, 0, numActiveSyncers)
	passiveSyncPeers := make([]*mockPeer, 0, numSyncers-numActiveSyncers)
	for i := 0; i < numSyncers; i++ {
		peer := randPeer(t, syncMgr.quit)
		if i < numActiveSyncers {
			activeSyncPeers = append(activeSyncPeers, peer)
		} else {
			passiveSyncPeers = append(passiveSyncPeers, peer)
		}
		syncMgr.InitSyncState(peer)
	}

	// Ensure that our expected active sync peers have an active syncer and
	// our passive sync peers have a passive syncer.
	for _, peer := range activeSyncPeers {
		assertSyncerStatus(t, syncMgr, peer, syncingChans, activeSync)
	}
	for _, peer := range passiveSyncPeers {
		assertSyncerStatus(
			t, syncMgr, peer, waitingQueryRangeReply, passiveSync,
		)
	}
}

// TestSyncManagerNewActiveSyncerAfterDisconnect ensures that we can regain an
// active syncer after losing one due to the peer disconnecting.
func TestSyncManagerNewActiveSyncerAfterDisconnect(t *testing.T) {
	t.Parallel()

	// We'll create our test sync manager to only have one active syncer.
	syncMgr := newTestSyncManager(1)
	syncMgr.Start()
	defer syncMgr.Stop()

	// peer1 will represent an active syncer, which will then be torn down
	// to simulate a disconnection. Since there are no other candidate
	// syncers available, the active syncer won't be replaced.
	peer1 := randPeer(t, syncMgr.quit)
	syncMgr.InitSyncState(peer1)
	assertMsgSent(t, peer1, &lnwire.QueryChannelRange{
		FirstBlockHeight: startHeight,
		NumBlocks:        math.MaxUint32 - startHeight,
	})
	syncMgr.PruneSyncState(peer1.PubKey())

	// Then, we'll start our active syncer again, but this time we'll also
	// have a passive syncer available to replace the active syncer after
	// the peer disconnects.
	syncMgr.InitSyncState(peer1)
	assertMsgSent(t, peer1, &lnwire.QueryChannelRange{
		FirstBlockHeight: startHeight,
		NumBlocks:        math.MaxUint32 - startHeight,
	})

	// Create our second peer, which should be intiialized as a passive
	// syncer.
	peer2 := randPeer(t, syncMgr.quit)
	syncMgr.InitSyncState(peer2)
	assertSyncerStatus(t, syncMgr, peer2, waitingQueryRangeReply, passiveSync)

	// We'll reset it's state to chansSynced to ensure it can process sync
	// transitions.
	assertTransitionToChansSynced(t, syncMgr, peer2)

	// Disconnect our active syncer, which should trigger the syncManager to
	// replace it with our passive syncer.
	syncMgr.PruneSyncState(peer1.PubKey())
	assertMsgSent(t, peer2, &lnwire.GossipTimestampRange{
		FirstTimestamp: uint32(time.Now().Unix()),
		TimestampRange: math.MaxUint32,
	})
	assertSyncerStatus(t, syncMgr, peer2, syncingChans, activeSync)
}

// TestSyncManagerRotateActiveSyncers tests that we can successfully rotate our
// active syncers after a certain interval.
func TestSyncManagerRotateActiveSyncers(t *testing.T) {
	t.Parallel()

	const numActiveSyncers = 3
	const numPassiveSyncers = 3

	// We'll create our sync manager with three active syncers.
	syncMgr := newTestSyncManager(numActiveSyncers)
	syncMgr.Start()
	defer syncMgr.Stop()

	activeSyncPeers := make([]*mockPeer, 0, numActiveSyncers)
	for i := 0; i < numActiveSyncers; i++ {
		peer := randPeer(t, syncMgr.quit)
		activeSyncPeers = append(activeSyncPeers, peer)
		syncMgr.InitSyncState(peer)
	}

	// The syncers can only process sync transitions when being in a
	// chansSynced state, so we'll make sure to override them with this
	// state.
	for _, peer := range activeSyncPeers {
		assertSyncerStatus(t, syncMgr, peer, syncingChans, activeSync)
		assertMsgSent(t, peer, &lnwire.QueryChannelRange{
			FirstBlockHeight: startHeight,
			NumBlocks:        math.MaxUint32 - startHeight,
		})
		assertTransitionToChansSynced(t, syncMgr, peer)
		assertActiveGossipTimestampRange(t, peer)
	}

	// We'll send a tick to force a rotation. Since there aren't any
	// candidates, none of the active syncers will be rotated.
	syncMgr.cfg.RotateTicker.(*ticker.Force).Force <- time.Time{}
	for _, peer := range activeSyncPeers {
		assertNoMsgSent(t, peer)
		assertSyncerStatus(t, syncMgr, peer, chansSynced, activeSync)
	}

	// We'll then go ahead and add our passive syncers.
	passiveSyncPeers := make([]*mockPeer, 0, numPassiveSyncers)
	for i := 0; i < numPassiveSyncers; i++ {
		peer := randPeer(t, syncMgr.quit)
		passiveSyncPeers = append(passiveSyncPeers, peer)
		syncMgr.InitSyncState(peer)

		assertSyncerStatus(
			t, syncMgr, peer, waitingQueryRangeReply, passiveSync,
		)

		// They also require being in a chansSynced state to be eligible
		// for a transition to an active syncer.
		assertTransitionToChansSynced(t, syncMgr, peer)
	}

	// We'll force another rotation, this time all of our active syncers
	// should become passive and all of our passive should become active.
	syncMgr.cfg.RotateTicker.(*ticker.Force).Force <- time.Time{}

	// The transition from an active syncer to a passive syncer causes the
	// peer to send out a new GossipTimestampRange in the past so that they
	// don't receive new graph updates.
	checkActiveSyncerTransition := func(peer *mockPeer) error {
		err := checkMsgSent(peer, &lnwire.GossipTimestampRange{
			FirstTimestamp: 0,
			TimestampRange: 0,
		})
		if err != nil {
			return err
		}
		return checkSyncerStatus(syncMgr, peer, chansSynced, passiveSync)
	}

	// Since we are not aware of the order the active syncers were chosen
	// to transition to passive syncers, we'll launch our assertions in a
	// goroutine for every peer and wait for the first non-nil error to
	// fail, if any.
	errChan := make(chan error, len(activeSyncPeers))
	for _, peer := range activeSyncPeers {
		go func(peer *mockPeer) {
			errChan <- checkActiveSyncerTransition(peer)
		}(peer)
	}
	for i := 0; i < len(activeSyncPeers); i++ {
		var err error
		select {
		case err = <-errChan:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for passive syncers check " +
				"to complete")
		}
		if err != nil {
			t.Fatal(err)
		}
	}

	// The transition from a passive syncer to an active syncer causes the
	// peer to send a new GossipTimestampRange with the current timestamp to
	// signal that they would like to receive new graph updates from their
	// peers. This will also cause the gossip syncer to redo its state
	// machine, starting from its initial syncingChans state. We'll then
	// need to transition it to its final chansSynced state to ensure the
	// next syncer is properly started in the round-robin.
	checkPassiveSyncerTransition := func(peer *mockPeer) error {
		err := checkActiveGossipTimestampRange(peer)
		if err != nil {
			return err
		}
		err = checkSyncerStatus(
			syncMgr, peer, syncingChans, activeSync,
		)
		if err != nil {
			return err
		}
		err = checkMsgSent(peer, &lnwire.QueryChannelRange{
			FirstBlockHeight: startHeight,
			NumBlocks:        math.MaxUint32 - startHeight,
		})
		if err != nil {
			return err
		}
		return transitionToChansSynced(syncMgr, peer)
	}

	// Once again, since we are not aware of the order the passive syncers
	// were chosen to transition to active syncers, we'll launch our
	// assertions in a goroutine for every peer and wait for the first
	// non-nil error to fail, if any.
	errChan = make(chan error, len(passiveSyncPeers))
	for _, peer := range passiveSyncPeers {
		go func(peer *mockPeer) {
			errChan <- checkPassiveSyncerTransition(peer)
		}(peer)
	}
	for i := 0; i < len(passiveSyncPeers); i++ {
		var err error
		select {
		case err = <-errChan:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for active syncers check " +
				"to complete")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestSyncManagerForceFullSync ensures that we can force a full sync with a
// non-active syncer when the FullSyncTicker ticks.
func TestSyncManagerForceFullSync(t *testing.T) {
	t.Parallel()

	// We'll create our sync manager with no active syncers as they are not
	// needed for this test.
	syncMgr := newTestSyncManager(0)
	syncMgr.Start()
	defer syncMgr.Stop()

	// We'll create a passive sync peer.
	peer := randPeer(t, syncMgr.quit)
	syncMgr.InitSyncState(peer)
	assertSyncerStatus(t, syncMgr, peer, waitingQueryRangeReply, passiveSync)

	// Then, we'll send a tick to force a full sync. This won't actually
	// trigger one since it requires the candidate syncer to be in a
	// chansSynced state.
	syncMgr.cfg.FullSyncTicker.(*ticker.Force).Force <- time.Time{}
	assertNoMsgSent(t, peer)
	assertSyncerStatus(t, syncMgr, peer, waitingQueryRangeReply, passiveSync)

	// We'll perform the transition and try again.
	assertTransitionToChansSynced(t, syncMgr, peer)
	syncMgr.cfg.FullSyncTicker.(*ticker.Force).Force <- time.Time{}

	// This time, it should perform a full sync, so we should expect to see
	// a QueryChannelRange message with a FirstBlockHeight of the genesis
	// block.
	assertSyncerStatus(t, syncMgr, peer, syncingChans, fullSync)
	assertMsgSent(t, peer, &lnwire.QueryChannelRange{
		FirstBlockHeight: 0,
		NumBlocks:        math.MaxUint32,
	})
}

// TestSyncManagerRoundRobin ensures that any subsequent active syncers can only
// be started after the previous one has completed its state machine.
func TestSyncManagerRoundRobin(t *testing.T) {
	t.Parallel()

	const numActiveSyncers = 3

	// We'll start by creating our sync manager with support for three
	// active syncers.
	syncMgr := newTestSyncManager(numActiveSyncers)
	syncMgr.Start()
	defer syncMgr.Stop()

	peers := make([]*mockPeer, 0, numActiveSyncers)
	for i := 0; i < numActiveSyncers; i++ {
		peer := randPeer(t, syncMgr.quit)
		syncMgr.InitSyncState(peer)
		peers = append(peers, peer)
	}

	// assertSyncerStarted ensures the target peer's syncer is the only that
	// has started.
	assertSyncerStarted := func(target *mockPeer) {
		t.Helper()

		for _, peer := range peers {
			if peer.PubKey() != target.PubKey() {
				assertNoMsgSent(t, peer)
				continue
			}

			assertMsgSent(t, peer, &lnwire.QueryChannelRange{
				FirstBlockHeight: startHeight,
				NumBlocks:        math.MaxUint32 - startHeight,
			})
		}
	}

	// startNextSyncer transitions the syncer for the given peer to its
	// final chansSynced state, allowing the next syncer in the queue to
	// start.
	startNextSyncer := func(peer *mockPeer) {
		t.Helper()

		assertTransitionToChansSynced(t, syncMgr, peer)
		assertActiveGossipTimestampRange(t, peer)
	}

	// The first syncer should start automatically, but the subsequent ones
	// should depend on the previous syncer to complete its state machine
	// first.
	for _, peer := range peers {
		assertSyncerStarted(peer)
		startNextSyncer(peer)
	}
}

// TestSyncManagerRoundRobinTimeout ensures that if we timeout while waiting for
// an active syncer to reach its final chansSynced state, then we will go on to
// start the next.
func TestSyncManagerRoundRobinTimeout(t *testing.T) {
	t.Parallel()

	// Create our sync manager with support for two active syncers.
	syncMgr := newTestSyncManager(2)
	syncMgr.Start()
	defer syncMgr.Stop()

	// peer1 will be the first peer we start, which will time out and cause
	// peer2 to start.
	peer1 := randPeer(t, syncMgr.quit)
	peer2 := randPeer(t, syncMgr.quit)

	syncMgr.InitSyncState(peer1)

	// We assume the syncer for peer1 has started once we see it send a
	// lnwire.QueryChannelRange message.
	assertMsgSent(t, peer1, &lnwire.QueryChannelRange{
		FirstBlockHeight: startHeight,
		NumBlocks:        math.MaxUint32 - startHeight,
	})

	// We'll then create the syncer for peer2. This should cause it to be
	// queued so that it starts once the syncer for peer1 is done.
	syncMgr.InitSyncState(peer2)
	assertNoMsgSent(t, peer2)

	// Send a force tick to pretend the sync manager has timed out waiting
	// for peer1's syncer to reach chansSynced.
	syncMgr.cfg.ActiveSyncerTimeoutTicker.(*ticker.Force).Force <- time.Time{}

	// Finally, ensure that the syncer for peer2 has started.
	assertMsgSent(t, peer2, &lnwire.QueryChannelRange{
		FirstBlockHeight: startHeight,
		NumBlocks:        math.MaxUint32 - startHeight,
	})
}

// TestSyncManagerRoundRobinStaleSyncer ensures that any stale active syncers we
// are currently waiting for or are queued up to start are properly removed and
// stopped.
func TestSyncManagerRoundRobinStaleSyncer(t *testing.T) {
	t.Parallel()

	const numActiveSyncers = 4

	// We'll create and start our sync manager with some active syncers.
	syncMgr := newTestSyncManager(numActiveSyncers)
	syncMgr.Start()
	defer syncMgr.Stop()

	peers := make([]*mockPeer, 0, numActiveSyncers)
	for i := 0; i < numActiveSyncers; i++ {
		peer := randPeer(t, syncMgr.quit)
		syncMgr.InitSyncState(peer)
		peers = append(peers, peer)
	}

	// assertSyncerStarted ensures the target peer's syncer is the only that
	// has started.
	assertSyncerStarted := func(target *mockPeer) {
		t.Helper()

		for _, peer := range peers {
			if peer.PubKey() != target.PubKey() {
				assertNoMsgSent(t, peer)
				continue
			}

			assertMsgSent(t, peer, &lnwire.QueryChannelRange{
				FirstBlockHeight: startHeight,
				NumBlocks:        math.MaxUint32 - startHeight,
			})
		}
	}

	// The first syncer should've started since it has no dependents.
	assertSyncerStarted(peers[0])

	// We'll then remove the syncers in the middle to cover the case where
	// they are queued up in the sync manager's pending list.
	for i, peer := range peers {
		if i == 0 || i == len(peers)-1 {
			continue
		}

		syncMgr.PruneSyncState(peer.PubKey())
	}

	// We'll then remove the syncer we are currently waiting for. This
	// should prompt the last syncer to start since it is the only one left
	// pending.
	syncMgr.PruneSyncState(peers[0].PubKey())
	assertSyncerStarted(peers[len(peers)-1])
}

// assertNoMsgSent is a helper function that ensures a peer hasn't sent any
// messages.
func assertNoMsgSent(t *testing.T, peer *mockPeer) {
	t.Helper()

	select {
	case msg := <-peer.sentMsgs:
		t.Fatalf("peer %x sent unexpected message %v", peer.PubKey(),
			spew.Sdump(msg))
	case <-time.After(time.Second):
	}
}

// checkMsgSent ensures that the peer has sent the given message.
func checkMsgSent(peer *mockPeer, msg lnwire.Message) error {
	var msgSent lnwire.Message
	select {
	case msgSent = <-peer.sentMsgs:
	case <-time.After(time.Second):
		return fmt.Errorf("expected peer %x to send %T message",
			peer.PubKey(), msg)
	}

	if !reflect.DeepEqual(msgSent, msg) {
		return fmt.Errorf("expected peer %x to send message: %v\ngot: %v",
			peer.PubKey(), spew.Sdump(msg), spew.Sdump(msgSent))
	}

	return nil
}

// assertMsgSent asserts that the peer has sent the given message.
func assertMsgSent(t *testing.T, peer *mockPeer, msg lnwire.Message) {
	t.Helper()

	if err := checkMsgSent(peer, msg); err != nil {
		t.Fatal(err)
	}
}

// checkActiveGossipTimestampRange ensures a peer has sent a
// lnwire.GossipTimestampRange message indicating that it would like to receive
// new graph updates.
func checkActiveGossipTimestampRange(peer *mockPeer) error {
	var msgSent lnwire.Message
	select {
	case msgSent = <-peer.sentMsgs:
	case <-time.After(time.Second):
		return fmt.Errorf("expected peer %x to send "+
			"*lnwire.GossipTimestampRange message", peer.PubKey())
	}

	msg, ok := msgSent.(*lnwire.GossipTimestampRange)
	if !ok {
		return fmt.Errorf("expected peer %x to send %T message",
			peer.PubKey(), msg)
	}
	if msg.FirstTimestamp == 0 {
		return errors.New("expected *lnwire.GossipTimestampRange " +
			"message with non-zero FirstTimestamp")
	}
	if msg.TimestampRange == 0 {
		return errors.New("expected *lnwire.GossipTimestampRange " +
			"message with non-zero TimestampRange")
	}

	return nil
}

// assertActiveGossipTimestampRange is a helper function that ensures a peer has
// sent a lnwire.GossipTimestampRange message indicating that it would like to
// receive new graph updates.
func assertActiveGossipTimestampRange(t *testing.T, peer *mockPeer) {
	t.Helper()

	if err := checkActiveGossipTimestampRange(peer); err != nil {
		t.Fatal(err)
	}
}

// checkSyncerStatus checks that the gossip syncer for the given peer matches
// the expected sync state and type.
func checkSyncerStatus(syncMgr *syncManager, peer *mockPeer,
	syncState syncerState, syncType syncerType) error {

	s, ok := syncMgr.GossipSyncer(peer.PubKey())
	if !ok {
		return fmt.Errorf("gossip syncer for peer %x not found",
			peer.PubKey())
	}

	// We'll check the status of our syncer within a WaitPredicate as some
	// sync transitions might cause this to be racy.
	var predErr error
	err := lntest.WaitPredicate(func() bool {
		state := s.SyncState()
		if s.SyncState() != syncState {
			predErr = fmt.Errorf("expected syncState %v for peer "+
				"%x, got %v", syncState, peer.PubKey(), state)
			return false
		}

		typ := s.SyncType()
		if s.SyncType() != syncType {
			predErr = fmt.Errorf("expected syncType %v for peer "+
				"%x, got %v", syncType, peer.PubKey(), typ)
			return false
		}

		return true
	}, time.Second)
	if err != nil {
		return predErr
	}

	return nil
}

// assertSyncerStatus asserts that the gossip syncer for the given peer matches
// the expected sync state and type.
func assertSyncerStatus(t *testing.T, syncMgr *syncManager, peer *mockPeer,
	syncState syncerState, syncType syncerType) {

	t.Helper()

	err := checkSyncerStatus(syncMgr, peer, syncState, syncType)
	if err != nil {
		t.Fatal(err)
	}
}

// transitionToChansSynced transitions a gossipSyncer to its final chansSynced
// state.
func transitionToChansSynced(syncMgr *syncManager, peer *mockPeer) error {
	s, ok := syncMgr.GossipSyncer(peer.PubKey())
	if !ok {
		return fmt.Errorf("gossip syncer for peer %x not found",
			peer.PubKey())
	}

	s.ProcessQueryMsg(&lnwire.ReplyChannelRange{Complete: 1}, nil)

	chanSeries := syncMgr.cfg.ChanSeries.(*mockChannelGraphTimeSeries)

	select {
	case <-chanSeries.filterReq:
		chanSeries.filterResp <- nil
	case <-time.After(2 * time.Second):
		return errors.New("expected to receive FilterKnownChanIDs " +
			"request")
	}

	var predErr error
	err := lntest.WaitPredicate(func() bool {
		state := syncerState(atomic.LoadUint32(&s.state))
		if state != chansSynced {
			predErr = fmt.Errorf("expected syncerState %v, got %v",
				chansSynced, state)
			return false
		}

		return true
	}, time.Second)
	if err != nil {
		return predErr
	}

	return nil
}

// assertTransitionToChansSynced asserts the transition of a gossipSyncer to its
// final chansSynced state.
func assertTransitionToChansSynced(t *testing.T, syncMgr *syncManager,
	peer *mockPeer) {

	t.Helper()

	if err := transitionToChansSynced(syncMgr, peer); err != nil {
		t.Fatal(err)
	}
}
