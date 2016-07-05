// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"path/filepath"
	"sync"

	"golang.org/x/net/context"
)

// TODO: Make JournalServer actually do something.

type tlfJournalBundle struct {
	enabled   bool
	bJournal  *bserverTlfJournal
	mdStorage *mdServerTlfStorage
}

type JournalServer struct {
	codec  Codec
	crypto cryptoPure
	dir    string
	kbpki  KBPKI

	delegateBlockServer BlockServer
	delegateMDServer    MDServer

	lock       sync.RWMutex
	tlfBundles map[TlfID]*tlfJournalBundle
}

func (j *JournalServer) EnableJournaling(tlfID TlfID) error {
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
	j.tlfBundles[tlfID] = &tlfJournalBundle{true, bJournal, mdStorage}
	return nil
}

func (j *JournalServer) DisableJournaling(tlfID TlfID) error {
	j.lock.Lock()
	defer j.lock.Unlock()
	bundle, ok := j.tlfBundles[tlfID]
	if !ok {
		return nil
	}

	return bundle.mdStorage.flushOne(j.delegateMDServer)
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
		if !ok || bundle.enabled == false {
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
	codec Codec, crypto cryptoPure, kbpki KBPKI, dir string,
	bserver BlockServer, mdServer MDServer) *JournalServer {
	jServer := JournalServer{
		codec:               codec,
		crypto:              crypto,
		kbpki:               kbpki,
		dir:                 dir,
		delegateBlockServer: bserver,
		delegateMDServer:    mdServer,
		tlfBundles:          make(map[TlfID]*tlfJournalBundle),
	}
	return &jServer
}
