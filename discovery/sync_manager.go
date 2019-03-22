package discovery

import (
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/lnpeer"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing"
	"github.com/lightningnetwork/lnd/ticker"
)

const (
	// DefaultSyncerRotationInterval is the default interval in which we'll
	// rotate our active syncers for new ones.
	DefaultSyncerRotationInterval = time.Hour

	// DefaultFullSyncInterval is the default interval in which we'll force
	// a full sync to ensure we have as much of the public network as
	// possible.
	DefaultFullSyncInterval = 24 * time.Hour

	// DefaultActiveSyncerTimeout is the default timeout interval in which
	// we'll wait until an active syncer has completed its state machine and
	// reached its final chansSynced state.
	DefaultActiveSyncerTimeout = 5 * time.Minute
)

// syncManagerCfg contains all of the dependencies required for the syncManager
// to carry out its duties.
type syncManagerCfg struct {
	// ChainHash is a hash that indicates the specific network of the active
	// chain.
	ChainHash chainhash.Hash

	// ChanSeries is an interface that provides access to a time series view
	// of the current known channel graph. Each gossipSyncer enabled peer
	// will utilize this in order to create and respond to channel graph
	// time series queries.
	ChanSeries ChannelGraphTimeSeries

	// NumActiveSyncers is the number of peers for which we should have
	// active syncers with. After reaching NumActiveSyncers, any future
	// gossip syncers will be passive.
	NumActiveSyncers int

	// RotateTicker is a ticker responsible for notifying the syncManager
	// when it should rotate its active syncers. Any active syncers with a
	// chansSynced state will be exchanged for a passive syncer in order to
	// ensure we don't keep syncing with the same peers.
	RotateTicker ticker.Ticker

	// FullSyncTicker is a ticker responsible for notifying the syncManager
	// when it should attempt a full sync with a gossip sync peer.
	FullSyncTicker ticker.Ticker

	// ActiveSyncerTimeoutTicker is a ticker responsible for notifying the
	// syncManager when it should attempt to start the next pending
	// activeSyncer due to the current one not completing its state machine
	// within the timeout.
	ActiveSyncerTimeoutTicker ticker.Ticker
}

// activeSyncer is an internal wrapper type over a gossipSyncer used within the
// syncManager to determine whether a sync transition to an activeSync type
// should be performed before attempting to start the syncer.
type activeSyncer struct {
	*gossipSyncer
	transition bool
	errChan    chan error
}

// syncManager is a subsystem of the gossiper that manages the gossip syncers
// for peers currently connected. When a new peer is connected, the manager will
// create its accompanying gossip syncer and determine whether it should have an
// activeSync or passiveSync sync type based on how many other gossip syncers
// are currently active. Any activeSync gossip syncers are started in a
// round-robin manner to ensure we're not syncing with multiple peers at the
// same time.
type syncManager struct {
	start sync.Once
	stop  sync.Once

	cfg syncManagerCfg

	// syncersMtx guards the access to the variables activeSyncers and
	// syncers below.
	syncersMtx sync.RWMutex

	// activeSyncers is the set of all syncers for which we are currently
	// receiving graph updates from. The number of possible active syncers
	// is bounded by NumActiveSyncers.
	activeSyncers map[routing.Vertex]*gossipSyncer

	// syncers is the set of all syncers for which we are not currently
	// receiving graph updates from.
	syncers map[routing.Vertex]*gossipSyncer

	// newActiveSyncers is a channel through which we'll send any new active
	// syncers that should be started in a round-robin manner.
	newActiveSyncers chan *activeSyncer

	// staleActiveSyncers is a channel through which we'll send any stale
	// active syncers that should be removed from the round-robin.
	staleActiveSyncers chan routing.Vertex

	// pendingActiveSyncers is the list of active syncers which are pending
	// to be started. Syncers will be added to this list through the
	// newActiveSyncers and staleActiveSyncers channels.
	pendingActiveSyncers []*activeSyncer

	wg   sync.WaitGroup
	quit chan struct{}
}

// newSyncManager constructs a new syncManager backed by the given config.
func newSyncManager(cfg *syncManagerCfg) *syncManager {
	return &syncManager{
		cfg:     *cfg,
		syncers: make(map[routing.Vertex]*gossipSyncer),
		activeSyncers: make(
			map[routing.Vertex]*gossipSyncer, cfg.NumActiveSyncers,
		),
		newActiveSyncers:   make(chan *activeSyncer),
		staleActiveSyncers: make(chan routing.Vertex),
		quit:               make(chan struct{}),
	}
}

// Start starts the syncManager in order to properly carry out its duties.
func (m *syncManager) Start() {
	m.start.Do(func() {
		m.wg.Add(1)
		go m.syncerHandler()
	})
}

// Stop stops the syncManager from performing its duties.
func (m *syncManager) Stop() {
	m.stop.Do(func() {
		close(m.quit)
		m.wg.Wait()

		for _, syncer := range m.syncers {
			syncer.Stop()
		}
	})
}

// enqueueActiveSyncer adds the given gossip syncer to the end of the
// round-robin queue.
func (m *syncManager) enqueueActiveSyncer(s *gossipSyncer, transition bool) {
	select {
	case m.newActiveSyncers <- &activeSyncer{
		gossipSyncer: s,
		transition:   transition,
	}:
	case <-m.quit:
	}
}

// dequeueActiveSyncer removes the syncer for the given peer from the
// round-robin queue.
func (m *syncManager) dequeueActiveSyncer(peer routing.Vertex) {
	select {
	case m.staleActiveSyncers <- peer:
	case <-m.quit:
	}
}

// syncerHandler is the syncManager's main event loop responsible for:
//
//   1. Finding new peers to receive graph updates from to ensure we don't only
//   receive them from the same set of peers.
//
//   2. Finding new peers to force a full sync with to ensure we have as much of
//   the public network as possible.
//
//   3. Managing the round-robin queue of our active syncers to ensure they
//   don't overlap and request the same set of channels, which significantly
//   reduces bandwidth usage.
//
// NOTE: This must be run as a goroutine.
func (m *syncManager) syncerHandler() {
	defer m.wg.Done()

	// We'll start all of our relevant tickers, except for our
	// ActiveSyncerTimeoutTicker, which should only be started when we start
	// an activeSyncer.
	m.cfg.RotateTicker.Resume()
	defer m.cfg.RotateTicker.Stop()
	m.cfg.FullSyncTicker.Resume()
	defer m.cfg.FullSyncTicker.Stop()
	defer m.cfg.ActiveSyncerTimeoutTicker.Stop()

	var (
		// current will hold the current activeSyncer we're waiting for
		// to complete its state machine.
		current *activeSyncer

		// startNext will be responsible for containing the signal of
		// when the current activeSyncer has completed its state
		// machine. This signal allows us to start the next pending
		// activeSyncer, if any.
		startNext chan struct{}
	)

	// startSyncer is a helper closure we'll use to start a given syncer. If
	// necessary, a sync transition to an activeSync sync type will be
	// performed to ensure we receive new graph updates from this peer.
	startSyncer := func(s *activeSyncer) error {
		if s.transition {
			err := s.ProcessSyncTransition(activeSync)
			if err != nil {
				return fmt.Errorf("unable to transition to "+
					"%v: %v", activeSync, err)
			}
			s.transition = false

			m.syncersMtx.Lock()
			m.activeSyncers[s.cfg.peerPub] = s.gossipSyncer
			delete(m.syncers, s.cfg.peerPub)
			m.syncersMtx.Unlock()
		}

		log.Debugf("Starting active syncer for peer=%x", s.cfg.peerPub)

		startNext = s.ResetSyncedSignal()
		m.cfg.ActiveSyncerTimeoutTicker.Resume()
		s.Start()

		return nil
	}

	// startNextSyncer is a helper closure that we'll use to start the next
	// syncer queued up. If there aren't any, this will act as a NOP.
	startNextSyncer := func() {
		current = m.nextPendingActiveSyncer()
		for current != nil {
			err := startSyncer(current)
			if err == nil {
				return
			}

			log.Errorf("Unable to start gossipSyncer(%x): %v",
				current.cfg.peerPub, err)

			current = m.nextPendingActiveSyncer()
		}

		startNext = nil
		m.cfg.ActiveSyncerTimeoutTicker.Pause()
	}

	for {
		select {
		// Our RotateTicker has ticked, so we'll attempt to rotate any
		// of our activeSyncers with randomly chosen ones.
		case <-m.cfg.RotateTicker.Ticks():
			m.rotateActiveSyncers()

		// Our FullSyncTicker has ticked, so we'll randomly select a
		// peer and force a full sync with them.
		case <-m.cfg.FullSyncTicker.Ticks():
			m.forceFullSync()

		// A new activeSyncer has been received, so we'll either attempt
		// to start it if the queue is empty or queue it so that we
		// don't attempt to sync with multiple peers at the same time.
		case s := <-m.newActiveSyncers:
			if current == nil {
				if err := startSyncer(s); err != nil {
					log.Errorf("Unable to start "+
						"gossipSyncer(%x): %v", err)
					continue
				}

				current = s
				continue
			}

			log.Debugf("Queueing next active syncer for peer=%x",
				s.cfg.peerPub)

			m.pendingActiveSyncers = append(m.pendingActiveSyncers, s)

		// A stale activeSyncer has been received, so we'll need to
		// remove them from our queue. If we are currently waiting for
		// its state machine to complete, we'll move on to the next
		// activeSyncer in the queue.
		case peer := <-m.staleActiveSyncers:
			if current != nil && peer == current.cfg.peerPub {
				startNextSyncer()
				continue
			}

			for i, activeSyncer := range m.pendingActiveSyncers {
				if activeSyncer.cfg.peerPub == peer {
					m.pendingActiveSyncers = append(
						m.pendingActiveSyncers[:i],
						m.pendingActiveSyncers[i+1:]...,
					)
					break
				}
			}

		// Our current active syncer has reached its terminal
		// chansSynced state, so we'll proceed to starting the next
		// pending active syncer if there is one.
		case <-startNext:
			log.Debugf("Received chansSynced signal from "+
				"gossipSyncer(%x)", current.cfg.peerPub)

			startNextSyncer()

		// We've timed out waiting for the current active syncer to
		// reach its terminal chansSynced state.
		case <-m.cfg.ActiveSyncerTimeoutTicker.Ticks():
			// If there are no pending active syncers left, then we
			// can just reset the timeout for the current one.
			if len(m.pendingActiveSyncers) == 0 {
				// TODO(wilmer): Restart syncer state machine?
				log.Debugf("Rescheduling active syncer timeout "+
					"for peer=%x", current.cfg.peerPub)
				continue
			}

			// Otherwise, we'll move on to the next active syncer in
			// the list and add the current one to the end of our
			// queue.
			log.Debugf("Timed out waiting for active syncer with "+
				"peer=%x to complete", current.cfg.peerPub)

			m.pendingActiveSyncers = append(
				m.pendingActiveSyncers, current,
			)

			startNextSyncer()

		case <-m.quit:
			return
		}
	}
}

// nextPendingActiveSyncer returns the next activeSyncer pending to be start. If
// there aren't any, then `nil` is returned.
func (m *syncManager) nextPendingActiveSyncer() *activeSyncer {
	switch len(m.pendingActiveSyncers) {
	case 0:
		return nil
	case 1:
		s := m.pendingActiveSyncers[0]
		m.pendingActiveSyncers = nil
		return s
	default:
		s := m.pendingActiveSyncers[0]
		m.pendingActiveSyncers = m.pendingActiveSyncers[1:]
		return s
	}
}

// rotateActiveSyncers rotates all currently eligible active syncers.
func (m *syncManager) rotateActiveSyncers() {
	m.syncersMtx.Lock()
	defer m.syncersMtx.Unlock()

	// If we don't have any candidates, we can return early.
	if len(m.syncers) == 0 {
		log.Debug("No candidates to rotate active syncers")
		return
	}

	// Otherwise, we do so we should attempt to rotate as many eligible
	// active syncers as possible.
	numRotated := 0
	blacklist := make(map[routing.Vertex]struct{})
	for _, s := range m.activeSyncers {
		// The active syncer must be in a chansSynced state in order to
		// process sync transitions.
		if s.SyncState() != chansSynced {
			continue
		}

		// If we have an active syncer available, we'll go ahead and
		// choose a random passive syncer to swap with. It's possible
		// for us to not find a valid candidate, so we'll exit this loop
		// in that case.
		candidate := m.chooseRandomSyncer(blacklist)
		if candidate == nil {
			break
		}

		// Otherwise, we have both a valid active syncer and candidate
		// to swap with, so we'll blacklist them to ensure they're not
		// chosen once again.
		blacklist[s.cfg.peerPub] = struct{}{}
		blacklist[candidate.cfg.peerPub] = struct{}{}

		// We can then transition each syncer to their respective new
		// sync type.
		if err := m.transitionSyncer(s, passiveSync); err != nil {
			log.Errorf("Unable to transition gossipSyncer(%x) to "+
				"%v: %v", s.cfg.peerPub, passiveSync, err)
			continue
		}
		m.transitionSyncer(candidate, activeSync)
		numRotated++
	}

	if numRotated == 0 {
		log.Debug("No valid candidates to rotate active syncers")
	} else {
		log.Debugf("Rotated %d active syncers", numRotated)
	}
}

// transitionSyncer transitions a syncer to a new syncType.
//
// NOTE: Transitions to an activeSync type are optimistic since we don't know
// exactly when the transition will happen due to the round robin, therefore the
// error does not need to be checked.
func (m *syncManager) transitionSyncer(s *gossipSyncer, newSyncType syncerType) error {
	log.Debugf("Requesting transition to %v for gossipSyncer(%x)",
		newSyncType, s.cfg.peerPub)

	// If we're transitioning to an activeSync type, we'll short-circuit the
	// process by queueing the syncer in our round-robin. Once it's this
	// syncer's turn, the transition will happen. If this transition fails,
	// then any new gossip syncers will be used as active syncers until
	// reaching NumActiveSyncers.
	if newSyncType == activeSync {
		go m.enqueueActiveSyncer(s, true)
		return nil
	}

	if err := s.ProcessSyncTransition(newSyncType); err != nil {
		return err
	}

	// If we happened to transition from an activeSync type, we'll also
	// ensure to dequeue it from our round-robin.
	if _, ok := m.activeSyncers[s.cfg.peerPub]; ok {
		m.syncers[s.cfg.peerPub] = s
		delete(m.activeSyncers, s.cfg.peerPub)
		go m.dequeueActiveSyncer(s.cfg.peerPub)
	}

	return nil
}

// forceFullSync chooses a syncer with a remote peer at random and forces a full
// sync with it.
func (m *syncManager) forceFullSync() {
	m.syncersMtx.Lock()
	defer m.syncersMtx.Unlock()

	candidatesChosen := make(map[routing.Vertex]struct{})
	s := m.chooseRandomSyncer(candidatesChosen)
	for s != nil {
		candidatesChosen[s.cfg.peerPub] = struct{}{}
		err := m.transitionSyncer(s, fullSync)
		if err == nil {
			break
		}

		log.Errorf("Unable to transition gossipSyncer(%x) to %v: %v",
			s.cfg.peerPub, fullSync, err)

		s = m.chooseRandomSyncer(candidatesChosen)
	}
}

// chooseRandomSyncer returns a random non-active syncer that's eligible for a
// sync transition. A blacklist can be used to skip any previously chosen
// candidates.
//
// NOTE: It's possible for a nil value to be returned if there are no eligible
// candidate syncers.
//
// NOTE: This method requires the syncersMtx read lock to be held.
func (m *syncManager) chooseRandomSyncer(
	blacklist map[routing.Vertex]struct{}) *gossipSyncer {

	for _, s := range m.syncers {
		if blacklist != nil {
			if _, ok := blacklist[s.cfg.peerPub]; ok {
				continue
			}
		}
		if s.SyncState() != chansSynced {
			continue
		}
		return s
	}
	return nil
}

// InitSyncState is called by outside sub-systems when a connection is
// established to a new peer that understands how to perform channel range
// queries. We'll allocate a new gossip syncer for it, and start any goroutines
// needed to handle new queries.
//
// TODO(wilmer): Only mark as activeSync if this isn't a channel peer.
func (m *syncManager) InitSyncState(peer lnpeer.Peer) {
	m.syncersMtx.Lock()
	defer m.syncersMtx.Unlock()

	// If we already have a syncer, then we'll exit early as we don't want
	// to override it.
	nodeID := routing.Vertex(peer.PubKey())
	if _, ok := m.gossipSyncer(nodeID); ok {
		return
	}

	log.Infof("Creating new gossipSyncer for peer=%x", nodeID[:])

	encoding := lnwire.EncodingSortedPlain
	s := newGossipSyncer(gossipSyncerCfg{
		chainHash:     m.cfg.ChainHash,
		peerPub:       nodeID,
		channelSeries: m.cfg.ChanSeries,
		encodingType:  encoding,
		chunkSize:     encodingTypeToChunkSize[encoding],
		sendToPeer: func(msgs ...lnwire.Message) error {
			return peer.SendMessage(false, msgs...)
		},
	})

	// If we've yet to reach our desired number of active syncers, then
	// we'll use this one.
	if len(m.activeSyncers) < m.cfg.NumActiveSyncers {
		s.syncType = uint32(activeSync)
		m.activeSyncers[nodeID] = s
		m.enqueueActiveSyncer(s, false)
		return
	}

	// Otherwise, the syncer should be initialized as passive.
	s.syncType = uint32(passiveSync)
	s.state = uint32(waitingQueryRangeReply)
	m.syncers[nodeID] = s
	s.Start()
}

// PruneSyncState is called by outside sub-systems once a peer that we were
// previously connected to has been disconnected. In this case we can stop the
// existing gossipSyncer assigned to the peer and free up resources.
func (m *syncManager) PruneSyncState(peer routing.Vertex) {
	m.syncersMtx.Lock()
	defer m.syncersMtx.Unlock()

	s, ok := m.gossipSyncer(peer)
	if !ok {
		return
	}

	log.Infof("Removing gossipSyncer for peer=%v", peer)

	// We'll start by stopping the gossipSyncer for the disconnected peer.
	s.Stop()

	// If it's a non-active syncer, then we can just exit now.
	if _, ok := m.syncers[s.cfg.peerPub]; ok {
		delete(m.syncers, s.cfg.peerPub)
		return
	}

	// Otherwise, we'll need to dequeue it from our pending active syncers
	// queue and find a new one to replace it.
	delete(m.activeSyncers, s.cfg.peerPub)
	m.dequeueActiveSyncer(s.cfg.peerPub)

	newActiveSyncer := m.chooseRandomSyncer(nil)
	if newActiveSyncer != nil {
		m.transitionSyncer(newActiveSyncer, activeSync)
	}
}

// GossipSyncer returns the associated gossip syncer of a peer.
func (m *syncManager) GossipSyncer(peer routing.Vertex) (*gossipSyncer, bool) {
	m.syncersMtx.RLock()
	defer m.syncersMtx.RUnlock()
	return m.gossipSyncer(peer)
}

// gossipSyncer returns the associated gossip syncer of a peer.
//
// NOTE: This method requires the syncersMtx read lock to be held.
func (m *syncManager) gossipSyncer(peer routing.Vertex) (*gossipSyncer, bool) {
	syncer, ok := m.syncers[peer]
	if ok {
		return syncer, true
	}
	syncer, ok = m.activeSyncers[peer]
	if ok {
		return syncer, true
	}
	return nil, false
}

// GossipSyncers returns all of the currently initialized gossip syncers.
func (m *syncManager) GossipSyncers() map[routing.Vertex]*gossipSyncer {
	m.syncersMtx.RLock()
	defer m.syncersMtx.RUnlock()

	numSyncers := len(m.syncers) + len(m.activeSyncers)
	syncers := make(map[routing.Vertex]*gossipSyncer, numSyncers)
	for _, syncer := range m.syncers {
		syncers[syncer.cfg.peerPub] = syncer
	}
	for _, syncer := range m.activeSyncers {
		syncers[syncer.cfg.peerPub] = syncer
	}

	return syncers
}
