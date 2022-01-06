package CaskDB

import "os"

// due to the particularity of list structure storage,
// it is necessary to use snapshots for garbage collection

func (db *DB) listSnapshot(mergePath string) error {
	idx := db.listIndex.idx
	keys := idx.GetAllKeys()
	mergedArchedFiles := make(map[uint32]*File)
	var mergedActiveFile *File

	for _, k := range keys {
		vals := idx.Range(k, 0, -1)
		for _, v := range vals {
			e := NewEntry([]byte(k), v, List, ListRPush, 0)
			if err := db.storeMerged(e, mergedArchedFiles, &mergedActiveFile); err != nil {
				return err
			}
		}
	}

	if mergedActiveFile != nil {

		// close and remove old file
		for _, f := range db.archedFiles[1] {
			if err := f.Close(true); err != nil {
				return err
			}
			if err := os.Remove(f.fd.Name()); err != nil {
				return err
			}
		}
		f := db.activeFiles[1]
		if err := f.Close(true); err != nil {
			return err
		}
		if err := os.Remove(f.fd.Name()); err != nil {
			return err
		}

		if err := db.buildFromMerged(mergedActiveFile, mergedArchedFiles, 1, mergePath); err != nil {
			return err
		}
	}

	return nil
}
