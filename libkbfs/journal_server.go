// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

// TODO: Make journalServer actually do something.

type journalServer struct {
	delegateBlockServer BlockServer
	delegateMDServer    MDServer
}

type journalBlockServer struct {
	jServer journalServer
	BlockServer
}

type journalMDServer struct {
	jServer journalServer
	MDServer
}

func (j journalServer) blockServer() journalBlockServer {
	return journalBlockServer{j, j.delegateBlockServer}
}

func (j journalServer) mdServer() journalMDServer {
	return journalMDServer{j, j.delegateMDServer}
}

func makeJournalServer(bserver BlockServer, mdServer MDServer) journalServer {
	jServer := journalServer{
		delegateBlockServer: bserver,
		delegateMDServer:    mdServer,
	}
	return jServer
}
