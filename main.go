package main

import (
	"bufio"
	"fmt"
	"hashServer/server"
	"hashServer/shardmap"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

var (
	m  shardmap.Map
	wg sync.WaitGroup
)

func scanFile(dbfile string) error {
	fmt.Println(dbfile)
	f, err := os.OpenFile(dbfile, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)

	var (
		splitter string = ","
		key      string
	)
	for sc.Scan() {
		key = strings.Split(sc.Text(), splitter)[1]
		m.SET(key, struct{}{})
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return nil
}

func LOAD(dirname string) error {
	log.Println("Walking the following DIR: ", dirname)
	return filepath.Walk(dirname, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}

		if strings.Contains(path, ".csv") {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				if e := scanFile(p); e != nil {
					log.Printf("%v\n", e)
				}
			}(path)
		}
		return nil
	})
}

func main() {
	runtime.GOMAXPROCS(0)
	dataLoc := os.Getenv("DBSTORE_PATH")
	if dataLoc == "" {
		dataLoc, _ = os.Getwd()
	}
	if !strings.HasSuffix(dataLoc, "/") {
		dataLoc = dataLoc + "/data/"
	} else {
		dataLoc = dataLoc + "data/"
	}
	LOAD(dataLoc)
	wg.Wait()
	debug.FreeOSMemory()
	server.InitServer(&m, dataLoc)
}
