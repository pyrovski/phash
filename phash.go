package phash

// TODO: add tests

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"gocv.io/x/gocv"
	cv_contrib "gocv.io/x/gocv/contrib"
)

type PHasher struct {
	DBFile    string
	DBTimeout time.Duration
	KeyFile   string // key filename for directories of images
	HashProcs int
}

// insertHashesQuery is used to insert hashes into the 'key_hashes' table.
// CREATE TABLE key_hashes(fullpath text, mtime text, frame integer, h1 bigint, h2 bigint, h3 bigint, h4 bigint);
const insertHashesQuery = "INSERT INTO key_hashes(fullpath, frame, h1, h2, h3, h4) values(?,?,?,?,?,?)"
const lookupHashesQuery = "select fullpath, frame from key_hashes where h1 = ? and h2 = ? and h3 = ? and h4 = ?"

type image struct {
	// full image path
	path  string
	img   gocv.Mat
	frame int
	hash  gocv.Mat
	// image filename with "-[0-9]+.jpg" removed
	key string
}

// getImages gets all images from a path into a stream
// TODO: make this recursive
// TODO: switch to directory walking in parallel ala https://www.oreilly.com/learning/run-strikingly-fast-parallel-file-searches-in-go-with-sync-errgroup
// TODO: pass flag value as argument
func (h *PHasher) getImages(p string, c chan *image, wg *sync.WaitGroup) {
	defer wg.Done()
	files, err := ioutil.ReadDir(p)
	if err != nil {
		log.Print(err)
		return
	}
	if len(files) == 0 {
		log.Printf("no files in %q", p)
		return
	}
	var fileKey string
	if h.KeyFile != "" {
		fullKeyFile := path.Join(p, h.KeyFile)
		log.Printf("reading key from %q", fullKeyFile)
		b, err := ioutil.ReadFile(fullKeyFile)
		if err != nil {
			log.Print(err)
			return
		}
		fileKey = string(b)
		if fileKey == "" {
			log.Print("expected nonempty key")
			return
		}
	}
	// TODO: get a hash of the file header, add to struct
	re := regexp.MustCompile("(.*)-([0-9]+)[.]jpg")
	for _, f := range files {
		fullPath := path.Join(p, f.Name())
		matches := re.FindStringSubmatch(f.Name())
		// TODO: support video files directly with goav
		// TODO: support tar files of images
		if matches == nil {
			// log.Printf("skipping file: %q; regex: %v", fullPath, re)
			continue
		}
		frame, err := strconv.Atoi(matches[2])
		if err != nil {
			log.Printf("skipping file: %q; failed to parse frame: %v", fullPath, matches)
			continue
		}
		log.Printf("reading file: %q", fullPath)
		img := &image{
			path:  fullPath,
			img:   gocv.IMRead(fullPath, gocv.IMReadGrayScale),
			frame: frame,
		}
		if h.KeyFile != "" {
			img.key = fileKey
		} else {
			img.key = path.Join(p, matches[1])
		}
		if img.img.Empty() {
			log.Print(fmt.Sprintf("empty image: %q", fullPath))
			continue
		}
		c <- img
	}
}

// processImages reads images from 'c', adds perceptual hashes, and writes the
// results to 'dbC'.
func processImages(c chan *image, wg *sync.WaitGroup, dbC chan *image) {
	defer wg.Done()
	hasher := cv_contrib.BlockMeanHash{}
	for img := range c {
		img.hash = gocv.NewMat()
		hasher.Compute(img.img, &img.hash)
		img.img.Close()
		// block mean hash: 1x32 bytes
		// log.Printf("%q hash: %v", img.path, img.hash.ToBytes())
		dbC <- img
	}
}

// unpackHash converts a 32-byte hash from byte slice to a uint32 array
func unpackHash(h []byte) []uint32 {
	result := make([]uint32, 4)
	buf := bytes.NewBuffer(h)
	for i := 0; i < 4; i++ {
		binary.Read(buf, binary.BigEndian, &result[i])
	}
	return result
}

// storeHashes reads images over 'dbC' and stores their hashes to 'db'.
func (h *PHasher) storeHashes(dbC chan *image, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	commitFrames := func(imgs []*image) error {
		defer wg.Done()
		tx, err := db.Begin()
		defer tx.Rollback()
		if err != nil {
			log.Print(err)
			return err
		}
		stmt, err := tx.Prepare(insertHashesQuery)
		if err != nil {
			log.Print(err)
			return err
		}
		for _, img := range imgs {
			if img == nil {
				return nil
			}
			// TODO: put this inner loop code in a function
			un := unpackHash(img.hash.ToBytes())
			img.hash.Close()
			log.Print(img.key, " ", img.frame)
			_, err = stmt.Exec(img.key, img.frame, un[0], un[1], un[2], un[3])
			if err != nil && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
				log.Print(err)
				return err
			}
		}
		err = tx.Commit()
		if err != nil {
			log.Print(err)
			return err
		}
		return nil
	}

	retry := func(f func() error, timeout time.Duration) error {
		start := time.Now()
		var err error = nil
		for ok := true; ok; ok = time.Now().Before(start.Add(timeout)) {
			err = f()
			if err == nil ||
				!strings.Contains(err.Error(), "database is locked") {
				return err
			}
		}
		return err
	}

	count := 0
	batch := 100
	imgs := make([]*image, 0, batch)
	for img := range dbC {
		imgs = append(imgs, img)
		if count%batch == 0 {
			log.Print("commit")
			wg.Add(1)
			imgsCopy := make([]*image, len(imgs))
			copy(imgsCopy, imgs)
			go retry(func() error { return commitFrames(imgsCopy) }, h.DBTimeout)
			imgs = make([]*image, 0, batch)
		}
		count++
	}
	wg.Add(1)
	go retry(func() error { return commitFrames(imgs) }, h.DBTimeout)
	// log.Print("done storing")
}

// printHashes prints hashes from images in 'dbC'.
func printHashes(dbC chan *image, wg *sync.WaitGroup) {
	defer wg.Done()
	for img := range dbC {
		un := unpackHash(img.hash.ToBytes())
		fmt.Printf("%v\t%v\n", img.path, un)
	}
}

// lookupHashes looks up hashes from images in 'dbC' in 'db' and prints the results.
func lookupHashes(dbC chan *image, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	stmt, err := db.Prepare(lookupHashesQuery)
	if err != nil {
		log.Print(err)
		return
	}
	lookupHash := func(img *image) {
		defer wg.Done()
		un := unpackHash(img.hash.ToBytes())
		rows, err := stmt.Query(un[0], un[1], un[2], un[3])
		if err != nil {
			log.Print(err)
			return
		}
		defer rows.Close()
		paths := make([]string, 0)
		frames := make([]int, 0)
		for rows.Next() {
			var filepath string
			var frame int
			if err := rows.Scan(&filepath, &frame); err != nil {
				log.Fatal(err)
			}
			paths = append(paths, filepath)
			frames = append(frames, frame)
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v:%v:%v:%v\n", img.path, un, paths, frames)
	}

	for img := range dbC {
		wg.Add(1)
		go lookupHash(img)
	}
}

type mode int

const (
	query mode = 0
	store mode = 1
	show  mode = 2
)

func (h *PHasher) LookupHashesInDirs(paths []string)  { h.pipeline(paths, query) }
func (h *PHasher) StoreHashesFromDirs(paths []string) { h.pipeline(paths, store) }
func (h *PHasher) PrintHashesInDirs(paths []string)   { h.pipeline(paths, show) }

func (h *PHasher) pipeline(paths []string, m mode) {
	db, err := sql.Open("sqlite3", h.DBFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	c := make(chan *image)
	dbC := make(chan *image)
	pg := &sync.WaitGroup{}
	rg := &sync.WaitGroup{}
	dg := &sync.WaitGroup{}
	if h.HashProcs <= 0 {
		h.HashProcs = runtime.NumCPU()
	}
	for i := 0; i < h.HashProcs; i++ {
		pg.Add(1)
		go processImages(c, pg, dbC)
	}
	for _, p := range paths {
		rg.Add(1)
		go h.getImages(p, c, rg)
	}
	dg.Add(1)
	switch m {
	case query:
		go lookupHashes(dbC, db, dg)
	case store:
		go h.storeHashes(dbC, db, dg)
	case show:
		go printHashes(dbC, dg)
	}
	rg.Wait()
	close(c)
	pg.Wait()
	close(dbC)
	dg.Wait()
}
