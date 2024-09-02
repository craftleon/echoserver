package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CONNECTION_IDLE_TIMEOUTMS = 30000
)

var (
	ExeDirPath string
	ListenIp   string
	ListenPort int
)

type ConnType int32

const (
	TCP_CONN = iota
	UDP_CONN
)

type ConnData struct {
	// atomic data, keep 64bit(8-bytes) alignment for 32-bit system compatibility
	InitTime           int64 // local connection setup time. immutable after created
	LastRemoteSendTime int64
	LastLocalSendTime  int64
	LastLocalRecvTime  int64

	sync.Mutex
	sync.WaitGroup

	LocalAddr  net.Addr
	RemoteAddr net.Addr

	connType ConnType
	closed   atomic.Bool

	idleTimeoutMs int
	nativeConn    net.Conn

	echoQueue chan *TupleData
}

func (conn *ConnData) Close() {
	if conn.closed.Load() {
		return
	}
	conn.nativeConn.Close()
	conn.closed.Store(true)
}

type TupleData struct {
	SrcIp     string
	DstIp     string
	SrcPort   int
	DstPort   int
	Timestamp time.Time
	msg       string
}

func (td *TupleData) String() string {
	return fmt.Sprintf("%s %s:%d -> %s:%d \"%s\"", td.Timestamp.Format("2006-01-02 15:04:05"), td.SrcIp, td.SrcPort, td.DstIp, td.DstPort, td.msg)
}

// need external connection
func GetLocalOutboundAddress() net.IP {
	con, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil
	}
	defer con.Close()

	addr := con.LocalAddr().(*net.UDPAddr)

	return addr.IP
}
