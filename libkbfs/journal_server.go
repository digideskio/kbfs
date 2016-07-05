// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"path/filepath"
	"sync"

	"github.com/keybase/client/go/logger"

	"golang.org/x/net/context"
)

type tlfJournalBundle struct {
	bJournal  *bserverTlfJournal
	mdStorage *mdServerTlfStorage
}

type JournalServer struct {
	codec  Codec
	crypto cryptoPure
	dir    string
	kbpki  KBPKI

	log      logger.Logger
	deferLog logger.Logger

	delegateBlockServer BlockServer
	delegateMDServer    MDServer

	lock       sync.RWMutex
	tlfBundles map[TlfID]*tlfJournalBundle
}

func (j *JournalServer) EnableJournaling(tlfID TlfID) (err error) {
	j.log.Debug("Enabling journaling for %s", tlfID)
	defer func() {
		j.deferLog.Debug("EnableJournaling error: %v", err)
	}()

	j.lock.Lock()
	defer j.lock.Unlock()
	_, ok := j.tlfBundles[tlfID]
	if ok {
		return nil
	}

	path := filepath.Join(j.dir, tlfID.String())
	// TODO: Have both objects share the same lock.
	bJournal, err := makeBserverTlfJournal(j.codec, j.crypto, path)
	if err != nil {
		return err
	}
	mdStorage := makeMDServerTlfStorage(j.codec, j.crypto, path)
	j.tlfBundles[tlfID] = &tlfJournalBundle{bJournal, mdStorage}
	return nil
}

func (j *JournalServer) Flush(tlfID TlfID) (err error) {
	j.log.Debug("Flushing %s", tlfID)
	defer func() {
		j.deferLog.Debug("Flush error: %v", err)
	}()
	j.lock.Lock()
	defer j.lock.Unlock()
	bundle, ok := j.tlfBundles[tlfID]
	if !ok {
		return nil
	}

	flushedEntries := 0
	for {
		flushed, err := bundle.mdStorage.flushOne(j.delegateMDServer)
		if err != nil {
			return err
		}
		if !flushed {
			break
		}
		flushedEntries++
	}

	j.log.Debug("Flushed %d entries", flushedEntries)

	return nil
}

type journalBlockServer struct {
	jServer *JournalServer
	BlockServer
}

type journalMDServer struct {
	jServer *JournalServer
	MDServer
}

func (j journalMDServer) Put(ctx context.Context, rmds *RootMetadataSigned) error {
	if rmds.MD.BID != NullBranchID {
		panic("Branches not supported yet")
	}

	bundle, ok := func() (*tlfJournalBundle, bool) {
		j.jServer.lock.RLock()
		defer j.jServer.lock.RUnlock()
		bundle, ok := j.jServer.tlfBundles[rmds.MD.ID]
		if !ok {
			return nil, false
		}
		return bundle, ok
	}()
	if ok {
		_, currentUID, err := j.jServer.kbpki.GetCurrentUserInfo(ctx)
		if err != nil {
			return MDServerError{err}
		}

		key, err := j.jServer.kbpki.GetCurrentCryptPublicKey(ctx)
		if err != nil {
			return MDServerError{err}
		}

		recordBranchID, err := bundle.mdStorage.put(currentUID, key.kid, rmds)
		if err != nil {
			return err
		}

		if recordBranchID {
			// TODO: Do something with branch ID.
		}

		return nil
	}

	return j.MDServer.Put(ctx, rmds)
}

func (j *JournalServer) blockServer() journalBlockServer {
	return journalBlockServer{j, j.delegateBlockServer}
}

func (j *JournalServer) mdServer() journalMDServer {
	return journalMDServer{j, j.delegateMDServer}
}

func makeJournalServer(
	codec Codec, crypto cryptoPure, kbpki KBPKI, log logger.Logger,
	dir string, bserver BlockServer, mdServer MDServer) *JournalServer {
	jServer := JournalServer{
		codec:               codec,
		crypto:              crypto,
		kbpki:               kbpki,
		log:                 log,
		deferLog:            log.CloneWithAddedDepth(1),
		dir:                 dir,
		delegateBlockServer: bserver,
		delegateMDServer:    mdServer,
		tlfBundles:          make(map[TlfID]*tlfJournalBundle),
	}
	return &jServer
}
