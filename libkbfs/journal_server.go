// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import "errors"

// TODO: Make JournalServer actually do something.

type JournalServer struct {
	delegateBlockServer BlockServer
	delegateMDServer    MDServer
}

func (j JournalServer) EnableJournaling(tlfID TlfID) error {
	return errors.New("Not implemented yet")
}

func (j JournalServer) DisableJournaling(tlfID TlfID) error {
	return errors.New("Not implemented yet")
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

func makeJournalServer(bserver BlockServer, mdServer MDServer) JournalServer {
	jServer := JournalServer{
		delegateBlockServer: bserver,
		delegateMDServer:    mdServer,
	}
	return jServer
}
