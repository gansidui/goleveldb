package leveldb

import (
	"bytes"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gansidui/goleveldb/leveldb/opt"
	"github.com/gansidui/goleveldb/leveldb/storage"
	"github.com/gansidui/goleveldb/leveldb/util"
)

type compactionPauseStorage struct {
	storage.Storage
	prefix  string
	armed   uint32
	paused  chan struct{}
	proceed chan struct{}
}

func (s *compactionPauseStorage) Log(message string) {
	s.Storage.Log(message)
	if strings.HasPrefix(message, s.prefix) && atomic.CompareAndSwapUint32(&s.armed, 1, 0) {
		close(s.paused)
		<-s.proceed
	}
}

func TestDB_CloseDuringCompaction(t *testing.T) {
	for _, phase := range []string{"memdb@flush", "table@build created"} {
		t.Run(phase, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "leveldb-close-during-compaction-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)

			stor, err := storage.OpenFile(dir, false)
			if err != nil {
				t.Fatal(err)
			}
			defer stor.Close()

			pausedStorage := &compactionPauseStorage{
				Storage: stor,
				prefix:  phase,
				paused:  make(chan struct{}),
				proceed: make(chan struct{}),
			}
			db, err := Open(pausedStorage, &opt.Options{
				WriteBuffer:            256 * 1024,
				CompactionL0Trigger:    1000,
				WriteL0SlowdownTrigger: 1000,
				WriteL0PauseTrigger:    2000,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			expected := make(map[string][]byte)
			for i := 0; i < 600; i++ {
				key, value := tkey(i), tval(i, 1024)
				if err := db.Put(key, value, nil); err != nil {
					t.Fatal(err)
				}
				expected[string(key)] = value
			}
			deletedKey := tkey(0)
			if err := db.Delete(deletedKey, nil); err != nil {
				t.Fatal(err)
			}
			delete(expected, string(deletedKey))

			updatedKey, updatedValue := tkey(1), []byte("updated")
			if err := db.Put(updatedKey, updatedValue, nil); err != nil {
				t.Fatal(err)
			}
			expected[string(updatedKey)] = updatedValue

			// Let setup flushes finish before pausing the explicit compaction.
			if err := db.compTriggerWait(db.mcompCmdC); err != nil {
				t.Fatal(err)
			}

			compact := func() error {
				return db.CompactRange(util.Range{})
			}
			if phase == "memdb@flush" {
				// Exercise a flush whose command has a waiting caller.
				compact = func() error {
					db.writeLockC <- struct{}{}
					defer func() {
						<-db.writeLockC
					}()

					_, err := db.rotateMem(0, true)
					return err
				}
			}

			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() {
					close(pausedStorage.proceed)
				})
			}
			defer release()

			atomic.StoreUint32(&pausedStorage.armed, 1)
			compacted := make(chan error, 1)
			go func() {
				compacted <- compact()
			}()
			select {
			case <-pausedStorage.paused:
			case err := <-compacted:
				t.Fatalf("compaction ended before %q: %v", phase, err)
			case <-time.After(5 * time.Second):
				t.Fatalf("compaction did not reach %q", phase)
			}

			closed := make(chan error, 1)
			go func() {
				closed <- db.Close()
			}()
			select {
			case <-db.closeC:
				release()
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not signal the compaction worker")
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close blocked after the compaction worker resumed")
			}
			select {
			case err := <-compacted:
				if err != ErrClosed {
					t.Fatalf("compaction returned %v, want ErrClosed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("compaction did not return after Close")
			}

			reopened, err := Open(stor, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()

			iter := reopened.NewIterator(nil, nil)
			defer iter.Release()
			for iter.Next() {
				key := string(iter.Key())
				value, exists := expected[key]
				if !exists || !bytes.Equal(iter.Value(), value) {
					t.Fatalf("unexpected record after reopening: %q", key)
				}
				delete(expected, key)
			}
			if err := iter.Error(); err != nil {
				t.Fatal(err)
			}
			if len(expected) != 0 {
				t.Fatalf("missing %d records after reopening", len(expected))
			}
		})
	}
}
