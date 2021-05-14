package main

import (
	"log"
	"net"
	"reflect"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

type epoll struct {
	fd          int
	connections map[int]net.Conn
	lock        *sync.RWMutex
}

func MkEpoll() (*epoll, error) {
	if fd, err := unix.EpollCreate1(0); err != nil {
		return nil, err
	} else {
		return &epoll{
			fd:          fd,
			lock:        &sync.RWMutex{},
			connections: make(map[int]net.Conn),
		}, nil
	}
}

func (e *epoll) Add(conn net.Conn) error {
	fd := websocketFD(conn)
	if err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.POLLIN | unix.POLLHUP, Fd: int32(fd)}); err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.connections[fd] = conn
	log.Printf("Total number of connections: %v", len(e.connections))
	return nil
}

func (e *epoll) Remove(conn net.Conn) error {
	fd := websocketFD(conn)
	if err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil); err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.connections, fd)
	log.Printf("Total number of connections: %v", len(e.connections))
	return nil
}

func (e *epoll) Wait() ([]net.Conn, error) {
	events := make([]unix.EpollEvent, 100)
	if n, err := unix.EpollWait(e.fd, events, 100); err != nil {
		return nil, err
	} else {
		e.lock.RLock()
		defer e.lock.RUnlock()
		var connections []net.Conn
		for i := 0; i < n; i++ {
			conn := e.connections[int(events[i].Fd)]
			connections = append(connections, conn)
		}
		return connections, nil
	}
}

func websocketFD(conn net.Conn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")

	return int(pfdVal.FieldByName("Sysfd").Int())
}
