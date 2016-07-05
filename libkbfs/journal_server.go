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

func (j *JournalServer) getBundle(tlfID TlfID) (*tlfJournalBundle, bool) {
	j.lock.RLock()
	defer j.lock.RUnlock()
	bundle, ok := j.tlfBundles[tlfID]
	if !ok {
		return nil, false
	}
	return bundle, ok
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

	flushedBlockEntries := 0
	for {
		flushed, err := bundle.bJournal.flushOne(
			j.delegateBlockServer, tlfID, j.log)
		if err != nil {
			return err
		}
		if !flushed {
			break
		}
		flushedBlockEntries++
	}

	flushedMDEntries := 0
	for {
		flushed, err := bundle.mdStorage.flushOne(
			j.delegateMDServer, j.log)
		if err != nil {
			return err
		}
		if !flushed {
			break
		}
		flushedMDEntries++
	}

	j.log.Debug("Flushed %d block entries and %d MD entries",
		flushedBlockEntries, flushedMDEntries)

	return nil
}

type journalBlockServer struct {
	jServer *JournalServer
	BlockServer
}

func (j journalBlockServer) Put(
	ctx context.Context, id BlockID, tlfID TlfID, context BlockContext,
	buf []byte, serverHalf BlockCryptKeyServerHalf) error {
	bundle, ok := j.jServer.getBundle(tlfID)
	if ok {
		return bundle.bJournal.putData(id, context, buf, serverHalf)
	}

	return j.BlockServer.Put(ctx, id, tlfID, context, buf, serverHalf)
}

func (j journalBlockServer) AddBlockReference(
	ctx context.Context, id BlockID, tlfID TlfID,
	context BlockContext) error {
	bundle, ok := j.jServer.getBundle(tlfID)
	if ok {
		return bundle.bJournal.addReference(id, context)
	}

	return j.BlockServer.AddBlockReference(ctx, id, tlfID, context)
}

func (j journalBlockServer) RemoveBlockReference(
	ctx context.Context, tlfID TlfID,
	contexts map[BlockID][]BlockContext) (
	liveCounts map[BlockID]int, err error) {
	bundle, ok := j.jServer.getBundle(tlfID)
	if ok {
		liveCounts = make(map[BlockID]int)
		for id, idContexts := range contexts {
			count, err := bundle.bJournal.removeReferences(id, idContexts)
			if err != nil {
				return nil, err
			}
			liveCounts[id] = count
		}
		// TODO: Reminder: fix the fact that liveCounts is bogus.
		return liveCounts, nil
	}

	return j.BlockServer.RemoveBlockReference(ctx, tlfID, contexts)
}

func (j journalBlockServer) ArchiveBlockReferences(
	ctx context.Context, tlfID TlfID,
	contexts map[BlockID][]BlockContext) error {
	bundle, ok := j.jServer.getBundle(tlfID)
	if ok {
		for id, idContexts := range contexts {
			err := bundle.bJournal.archiveReferences(id, idContexts)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return j.BlockServer.ArchiveBlockReferences(ctx, tlfID, contexts)
}

type journalMDServer struct {
	jServer *JournalServer
	MDServer
}

func (j journalMDServer) Put(ctx context.Context, rmds *RootMetadataSigned) error {
	if rmds.MD.BID != NullBranchID {
		panic("Branches not supported yet")
	}

	bundle, ok := j.jServer.getBundle(rmds.MD.ID)
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
