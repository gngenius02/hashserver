package main

import (
	"log"
	"os"
)

type WriteFileStruct struct {
	Data  string
	Where string
}

func Write2File(data WriteFileStruct) error {
	var (
		f   *os.File
		err error
	)
	if f, err = os.OpenFile(data.Where, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		log.Println(err)
		return err
	}
	defer f.Close()
	if _, err = f.WriteString(data.Data + "\n"); err != nil {
		log.Println(err)
		return err
	}
	return nil
}
