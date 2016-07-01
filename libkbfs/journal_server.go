// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"path/filepath"
	"sync"
)

// TODO: Make JournalServer actually do something.

type tlfJournalBundle struct {
	bJournal  *bserverTlfJournal
	mdStorage *mdServerTlfStorage
}

type JournalServer struct {
	codec  Codec
	crypto cryptoPure
	dir    string

	delegateBlockServer BlockServer
	delegateMDServer    MDServer

	lock       sync.RWMutex
	tlfBundles map[TlfID]tlfJournalBundle
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
	j.tlfBundles[tlfID] = tlfJournalBundle{bJournal, mdStorage}
	return nil
}

func (j *JournalServer) DisableJournaling(tlfID TlfID) error {
	j.lock.Lock()
	defer j.lock.Unlock()
	_, ok := j.tlfBundles[tlfID]
	if !ok {
		return nil
	}

	// TODO: Flush to server before turning off.

	delete(j.tlfBundles, tlfID)
	return nil
}

type journalBlockServer struct {
	jServer JournalServer
	BlockServer
}

type journalMDServer struct {
	jServer JournalServer
	MDServer
}

func (j JournalServer) blockServer() journalBlockServer {
	return journalBlockServer{j, j.delegateBlockServer}
}

func (j JournalServer) mdServer() journalMDServer {
	return journalMDServer{j, j.delegateMDServer}
}

func makeJournalServer(
	codec Codec, crypto cryptoPure, dir string,
	bserver BlockServer, mdServer MDServer) *JournalServer {
	jServer := JournalServer{
		codec:               codec,
		crypto:              crypto,
		dir:                 dir,
		delegateBlockServer: bserver,
		delegateMDServer:    mdServer,
	}
	return &jServer
}
