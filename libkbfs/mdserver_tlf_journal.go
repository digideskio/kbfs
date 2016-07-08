// Copyright 2016 Keybase Inc. All rights reserved.
// Use of this source code is governed by a BSD
// license that can be found in the LICENSE file.

package libkbfs

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/keybase/client/go/logger"

	"golang.org/x/net/context"

	keybase1 "github.com/keybase/client/go/protocol"
)

// mdServerTlfJournal stores a single ordered list of metadata IDs for
// a single TLF, along with the associated metadata objects, in flat
// files on disk.
//
// The directory layout looks like:
//
// dir/md_journal/EARLIEST
// dir/md_journal/LATEST
// dir/md_journal/0...001
// dir/md_journal/0...002
// dir/md_journal/0...fff
// dir/mds/0100/0...01
// ...
// dir/mds/01ff/f...ff
//
// There's a single journal subdirectory; the journal ordinals are
// just MetadataRevisions, and the journal entries are just MdIDs.
//
// The Metadata objects are stored separately in dir/mds. Each block
// has its own subdirectory with its ID as a name. The MD
// subdirectories are splayed over (# of possible hash types) * 256
// subdirectories -- one byte for the hash type (currently only one)
// plus the first byte of the hash data -- using the first four
// characters of the name to keep the number of directories in dir
// itself to a manageable number, similar to git.
type mdServerTlfJournal struct {
	codec  Codec
	crypto cryptoPure
	dir    string

	// Protects any IO operations in dir or any of its children,
	// as well as branchJournals and its contents.
	//
	// TODO: Consider using https://github.com/pkg/singlefile
	// instead.
	lock       sync.RWMutex
	isShutdown bool
	j          mdServerBranchJournal
}

func makeMDServerTlfJournal(
	codec Codec, crypto cryptoPure, dir string) *mdServerTlfJournal {
	journal := &mdServerTlfJournal{
		codec:  codec,
		crypto: crypto,
		dir:    dir,
		j:      makeMDServerBranchJournal(codec, dir),
	}
	return journal
}

// The functions below are for building various paths.

func (s *mdServerTlfJournal) journalPath() string {
	return filepath.Join(s.dir, "md_journal")
}

func (s *mdServerTlfJournal) mdsPath() string {
	return filepath.Join(s.dir, "mds")
}

func (s *mdServerTlfJournal) mdPath(id MdID) string {
	idStr := id.String()
	return filepath.Join(s.mdsPath(), idStr[:4], idStr[4:])
}

// getDataLocked verifies the MD data (but not the signature) for the
// given ID and returns it.
//
// TODO: Verify signature?
func (s *mdServerTlfJournal) getMDReadLocked(id MdID) (
	*RootMetadataSigned, error) {
	// Read file.

	path := s.mdPath(id)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var rmds RootMetadataSigned
	err = s.codec.Decode(data, &rmds)
	if err != nil {
		return nil, err
	}

	// Check integrity.

	mdID, err := rmds.MD.MetadataID(s.crypto)
	if err != nil {
		return nil, err
	}

	if id != mdID {
		return nil, fmt.Errorf(
			"Metadata ID mismatch: expected %s, got %s", id, mdID)
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	rmds.untrustedServerTimestamp = fileInfo.ModTime()

	return &rmds, nil
}

func (s *mdServerTlfJournal) putMDLocked(rmds *RootMetadataSigned) error {
	id, err := rmds.MD.MetadataID(s.crypto)
	if err != nil {
		return err
	}

	_, err = s.getMDReadLocked(id)
	if os.IsNotExist(err) {
		// Continue on.
	} else if err != nil {
		return err
	} else {
		// Entry exists, so nothing else to do.
		return nil
	}

	path := s.mdPath(id)

	err = os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return err
	}

	buf, err := s.codec.Encode(rmds)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, buf, 0600)
}

func (s *mdServerTlfJournal) getHeadForTLFReadLocked() (
	rmds *RootMetadataSigned, err error) {
	headID, err := s.j.getHead()
	if err != nil {
		return nil, err
	}
	if headID == (MdID{}) {
		return nil, nil
	}
	return s.getMDReadLocked(headID)
}

func (s *mdServerTlfJournal) checkGetParamsReadLocked(
	currentUID keybase1.UID, deviceKID keybase1.KID) error {
	mergedMasterHead, err := s.getHeadForTLFReadLocked()
	if err != nil {
		return MDServerError{err}
	}

	ok, err := isReader(currentUID, mergedMasterHead)
	if err != nil {
		return MDServerError{err}
	}
	if !ok {
		return MDServerErrorUnauthorized{}
	}

	return nil
}

func (s *mdServerTlfJournal) getRangeReadLocked(
	currentUID keybase1.UID, deviceKID keybase1.KID,
	start, stop MetadataRevision) (
	[]*RootMetadataSigned, error) {
	err := s.checkGetParamsReadLocked(currentUID, deviceKID)
	if err != nil {
		return nil, err
	}

	realStart, mdIDs, err := s.j.getRange(start, stop)
	if err != nil {
		return nil, err
	}
	var rmdses []*RootMetadataSigned
	for i, mdID := range mdIDs {
		expectedRevision := realStart + MetadataRevision(i)
		rmds, err := s.getMDReadLocked(mdID)
		if err != nil {
			return nil, MDServerError{err}
		}
		if expectedRevision != rmds.MD.Revision {
			panic(fmt.Errorf("expected revision %v, got %v",
				expectedRevision, rmds.MD.Revision))
		}
		rmdses = append(rmdses, rmds)
	}

	return rmdses, nil
}

func (s *mdServerTlfJournal) isShutdownReadLocked() bool {
	return s.isShutdown
}

// All functions below are public functions.

var errMDServerTlfJournalShutdown = errors.New("mdServerTlfJournal is shutdown")

func (s *mdServerTlfJournal) journalLength() (uint64, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.isShutdownReadLocked() {
		return 0, errMDServerTlfJournalShutdown
	}

	return s.j.journalLength()
}

func (s *mdServerTlfJournal) getForTLF(
	currentUID keybase1.UID, deviceKID keybase1.KID) (
	*RootMetadataSigned, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.isShutdownReadLocked() {
		return nil, errMDServerTlfJournalShutdown
	}

	err := s.checkGetParamsReadLocked(currentUID, deviceKID)
	if err != nil {
		return nil, err
	}

	rmds, err := s.getHeadForTLFReadLocked()
	if err != nil {
		return nil, MDServerError{err}
	}
	return rmds, nil
}

func (s *mdServerTlfJournal) getRange(
	currentUID keybase1.UID, deviceKID keybase1.KID,
	start, stop MetadataRevision) ([]*RootMetadataSigned, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.isShutdownReadLocked() {
		return nil, errMDServerTlfJournalShutdown
	}

	return s.getRangeReadLocked(currentUID, deviceKID, start, stop)
}

func (s *mdServerTlfJournal) put(
	currentUID keybase1.UID, deviceKID keybase1.KID,
	rmds *RootMetadataSigned) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isShutdownReadLocked() {
		return errMDServerTlfJournalShutdown
	}

	mStatus := rmds.MD.MergedStatus()
	bid := rmds.MD.BID

	if (mStatus == Merged) != (bid == NullBranchID) {
		return MDServerErrorBadRequest{Reason: "Invalid branch ID"}
	}

	// Check permissions

	mergedMasterHead, err := s.getHeadForTLFReadLocked()
	if err != nil {
		return MDServerError{err}
	}

	ok, err := isWriterOrValidRekey(
		s.codec, currentUID, mergedMasterHead, rmds)
	if err != nil {
		return MDServerError{err}
	}
	if !ok {
		return MDServerErrorUnauthorized{}
	}

	head, err := s.getHeadForTLFReadLocked()
	if err != nil {
		return MDServerError{err}
	}

	if mStatus == Unmerged && head == nil {
		// currHead for unmerged history might be on the main branch
		prevRev := rmds.MD.Revision - 1
		rmdses, err := s.getRangeReadLocked(
			currentUID, deviceKID, prevRev, prevRev)
		if err != nil {
			return MDServerError{err}
		}
		if len(rmdses) != 1 {
			return MDServerError{
				Err: fmt.Errorf("Expected 1 MD block got %d", len(rmdses)),
			}
		}
		head = rmdses[0]
	}

	// Consistency checks
	if head != nil {
		err := head.MD.CheckValidSuccessorForServer(s.crypto, &rmds.MD)
		if err != nil {
			return err
		}
	}

	err = s.putMDLocked(rmds)
	if err != nil {
		return MDServerError{err}
	}

	id, err := rmds.MD.MetadataID(s.crypto)
	if err != nil {
		return MDServerError{err}
	}

	err = s.j.append(rmds.MD.Revision, id)
	if err != nil {
		return MDServerError{err}
	}

	return nil
}

func (s *mdServerTlfJournal) flushOne(
	mdOps MDOps, log logger.Logger) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isShutdownReadLocked() {
		return false, errMDServerTlfJournalShutdown
	}

	earliestID, err := s.j.getEarliest()
	if err != nil {
		return false, err
	}
	if earliestID == (MdID{}) {
		return false, nil
	}

	rmd, err := s.getMDReadLocked(earliestID)
	if err != nil {
		return false, err
	}

	log.Debug("Flushing MD put id=%s, rev=%s", earliestID, rmd.MD.Revision)

	if rmd.MD.MergedStatus() == Merged {
		err = mdOps.Put(context.Background(), &rmd.MD)
		if err != nil {
			return false, err
		}
	} else {
		err = mdOps.PutUnmerged(context.Background(), &rmd.MD)
		if err != nil {
			return false, err
		}
	}

	err = s.j.removeEarliest()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *mdServerTlfJournal) shutdown() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.isShutdown = true
}
