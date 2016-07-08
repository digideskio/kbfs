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
	lock         sync.RWMutex
	isShutdown   bool
	j            mdServerBranchJournal
	justBranched bool
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
func (s *mdServerTlfJournal) getMDReadLocked(id MdID) (*RootMetadata, error) {
	// Read file.

	path := s.mdPath(id)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var rmd RootMetadata
	err = s.codec.Decode(data, &rmd)
	if err != nil {
		return nil, err
	}

	// Check integrity.

	mdID, err := rmd.MetadataID(s.crypto)
	if err != nil {
		return nil, err
	}

	if id != mdID {
		return nil, fmt.Errorf(
			"Metadata ID mismatch: expected %s, got %s", id, mdID)
	}

	return &rmd, nil
}

func (s *mdServerTlfJournal) putMDLocked(rmd *RootMetadata) error {
	id, err := rmd.MetadataID(s.crypto)
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

	buf, err := s.codec.Encode(rmd)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, buf, 0600)
}

func (s *mdServerTlfJournal) getHeadReadLocked() (
	rmd *RootMetadata, err error) {
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
	currentUID keybase1.UID) error {
	head, err := s.getHeadReadLocked()
	if err != nil {
		return MDServerError{err}
	}

	ok, err := isReader(currentUID, head)
	if err != nil {
		return MDServerError{err}
	}
	if !ok {
		return MDServerErrorUnauthorized{}
	}

	return nil
}

func (s *mdServerTlfJournal) getRangeReadLocked(
	currentUID keybase1.UID, start, stop MetadataRevision) (
	[]*RootMetadata, error) {
	err := s.checkGetParamsReadLocked(currentUID)
	if err != nil {
		return nil, err
	}

	realStart, mdIDs, err := s.j.getRange(start, stop)
	if err != nil {
		return nil, err
	}
	var rmds []*RootMetadata
	for i, mdID := range mdIDs {
		expectedRevision := realStart + MetadataRevision(i)
		rmd, err := s.getMDReadLocked(mdID)
		if err != nil {
			return nil, MDServerError{err}
		}
		if expectedRevision != rmd.Revision {
			panic(fmt.Errorf("expected revision %v, got %v",
				expectedRevision, rmd.Revision))
		}
		rmds = append(rmds, rmd)
	}

	return rmds, nil
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

func (s *mdServerTlfJournal) get(
	currentUID keybase1.UID) (*RootMetadata, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.isShutdownReadLocked() {
		return nil, errMDServerTlfJournalShutdown
	}

	err := s.checkGetParamsReadLocked(currentUID)
	if err != nil {
		return nil, err
	}

	rmd, err := s.getHeadReadLocked()
	if err != nil {
		return nil, MDServerError{err}
	}
	return rmd, nil
}

func (s *mdServerTlfJournal) getRange(
	currentUID keybase1.UID, start, stop MetadataRevision) ([]*RootMetadata, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.isShutdownReadLocked() {
		return nil, errMDServerTlfJournalShutdown
	}

	return s.getRangeReadLocked(currentUID, start, stop)
}

func (s *mdServerTlfJournal) put(
	currentUID keybase1.UID, rmd *RootMetadata) (err error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.isShutdownReadLocked() {
		return errMDServerTlfJournalShutdown
	}

	mStatus := rmd.MergedStatus()
	bid := rmd.BID

	if (mStatus == Merged) != (bid == NullBranchID) {
		return MDServerErrorBadRequest{Reason: "Invalid branch ID"}
	}

	// Check permissions

	head, err := s.getHeadReadLocked()
	if err != nil {
		return MDServerError{err}
	}

	ok, err := isWriterOrValidRekey(s.codec, currentUID, head, rmd)
	if err != nil {
		return MDServerError{err}
	}
	if !ok {
		return MDServerErrorUnauthorized{}
	}

	if mStatus == Merged {
		if s.justBranched {
			rmd.WFlags |= MetadataFlagUnmerged
			rmd.BID = head.BID
			headID, err := head.MetadataID(s.crypto)
			if err != nil {
				return MDServerError{err}
			}
			rmd.PrevRoot = headID
			s.justBranched = false
		} else if head.MergedStatus() != Merged {
			// Conflict resolution shouldn't happen.
			return MDServerError{
				Err: errors.New("Shouldn't be doing conflict res"),
			}
		}
	} else {
		if head == nil {
			return MDServerError{
				Err: errors.New("Unexpectedly unmerged while empty"),
			}
		}
	}

	// Consistency checks
	if head != nil {
		err := head.CheckValidSuccessorForServer(s.crypto, rmd)
		if err != nil {
			return err
		}
	}

	err = s.putMDLocked(rmd)
	if err != nil {
		return MDServerError{err}
	}

	id, err := rmd.MetadataID(s.crypto)
	if err != nil {
		return MDServerError{err}
	}

	err = s.j.append(rmd.Revision, id)
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

	log.Debug("Flushing MD put id=%s, rev=%s", earliestID, rmd.Revision)

	if rmd.MergedStatus() == Merged {
		err = mdOps.Put(context.Background(), rmd)
		var fakeFBO *folderBranchOps
		doUnmergedPut := fakeFBO.isRevisionConflict(err)
		if doUnmergedPut {
			earliestRevision, err := s.j.readEarliestRevision()
			if err != nil {
				return false, err
			}

			latestRevision, err := s.j.readLatestRevision()
			if err != nil {
				return false, err
			}

			log.Debug("Conflict detected; rewriting MDs %s to %s",
				earliestRevision, latestRevision)

			start, allMdIDs, err := s.j.getRange(earliestRevision, latestRevision)
			if err != nil {
				return false, err
			}

			bid, err := s.crypto.MakeRandomBranchID()
			if err != nil {
				return false, err
			}

			log.Debug("New branch ID=%s", bid)

			// TODO: Do the below atomically.

			for i, id := range allMdIDs {
				rmd, err := s.getMDReadLocked(id)
				if err != nil {
					return false, err
				}
				rmd.WFlags |= MetadataFlagUnmerged
				rmd.BID = bid

				err = s.putMDLocked(rmd)
				if err != nil {
					return false, err
				}

				rev := start + MetadataRevision(i)
				o, err := revisionToOrdinal(rev)
				if err != nil {
					return false, err
				}

				newID, err := rmd.MetadataID(s.crypto)
				if err != nil {
					return false, err
				}

				err = s.j.j.writeJournalEntry(o, newID)
				if err != nil {
					return false, err
				}

				log.Debug("Changing ID for rev=%s from %s to %s",
					rev, id, newID)
			}

			earliestID, err := s.j.getEarliest()
			if err != nil {
				return false, err
			}

			rmd, err := s.getMDReadLocked(earliestID)
			if err != nil {
				return false, err
			}

			err = mdOps.PutUnmerged(context.Background(), rmd)
			if err != nil {
				return false, err
			}

			s.justBranched = true
		} else if err != nil {
			return false, err
		}
	} else {
		err = mdOps.PutUnmerged(context.Background(), rmd)
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
