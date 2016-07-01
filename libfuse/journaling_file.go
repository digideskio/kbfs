// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libfuse

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/keybase/kbfs/libkbfs"
	"golang.org/x/net/context"
)

type JournalingFile struct {
	folder *Folder
	enable bool
}

var _ fs.Node = (*JournalingFile)(nil)

// Attr implements the fs.Node interface for JournalingFile.
func (f *JournalingFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Size = 0
	a.Mode = 0222
	return nil
}

var _ fs.Handle = (*JournalingFile)(nil)

var _ fs.HandleWriter = (*JournalingFile)(nil)

// Write implements the fs.HandleWriter interface for JournalingFile.
func (f *JournalingFile) Write(ctx context.Context, req *fuse.WriteRequest,
	resp *fuse.WriteResponse) (err error) {
	f.folder.fs.log.CDebugf(ctx, "JournalingFile (enable: %t) Write", f.enable)
	defer func() { f.folder.reportErr(ctx, libkbfs.WriteMode, err) }()
	if len(req.Data) == 0 {
		return nil
	}

	jServer, err := libkbfs.GetJournalServer(f.folder.fs.config)
	if err != nil {
		return err
	}

	if f.enable {
		err := jServer.EnableJournaling(f.folder.getFolderBranch().Tlf)
		if err != nil {
			return err
		}
	} else {
		jServer.DisableJournaling(f.folder.getFolderBranch().Tlf)
		if err != nil {
			return err
		}
	}

	resp.Size = len(req.Data)
	return nil
}
