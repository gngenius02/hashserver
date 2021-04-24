package main

import (
	"os"
)

type Fs struct {
	file *os.File
}

func FileOpen(path string) (*Fs, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	return &Fs{file}, err
}

func (f *Fs) Write2File(d string) error {
	if _, err := f.file.WriteString(d + "\n"); err != nil {
		return err
	}
	return nil
}

func (f *Fs) CloseFile() error {
	return f.file.Close()
}
