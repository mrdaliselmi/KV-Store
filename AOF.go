package main

import (
	"bufio"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	go aof.syncToDisk()

	return aof, nil
}

func (a *Aof) syncToDisk() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		a.mu.Lock()
		a.file.Sync()
		a.mu.Unlock()
	}
}

func (a *Aof) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

func (a *Aof) Write(value Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Write(value.Marshal())
	return err
}

func (a *Aof) Read(fn func(value Value)) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	reader := NewResp(a.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		fn(value)
	}

	return nil
}
