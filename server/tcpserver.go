package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type TcpServer struct {
	stats struct {
		totalRecvBytes uint64
		totalSendBytes uint64
	}

	listenAddr *net.TCPAddr
	listenConn *net.TCPListener

	wg      sync.WaitGroup
	running atomic.Bool

	// connection and remote transaction management

	remoteConnectionMapMutex sync.Mutex
	remoteConnectionMap      map[string]*ConnData // indexed by remote UDP address

	// signals
	signals struct {
		stop chan struct{}
	}
}

func (s *TcpServer) Start(addr string, port int) (err error) {
	if port < 5000 || port > 65535 {
		return fmt.Errorf("listening port must be in the range of 5000-65535")
	}

	ListenPort = port

	var netIP net.IP
	if len(addr) > 0 {
		netIP = net.ParseIP(addr)
		if netIP == nil {
			return fmt.Errorf("listening ip address is incorrect")
		}
		ListenIp = addr
	} else {
		netIP = net.IPv4zero // will both listen on ipv4 0.0.0.0:port and ipv6 [::]:port
		// retrieve local ip
		ListenIp = GetLocalOutboundAddress().String()
	}

	s.listenConn, err = net.ListenTCP("tcp", &net.TCPAddr{
		IP:   netIP,
		Port: port,
	})
	if err != nil {
		return fmt.Errorf("listen error %v", err)
	}

	// retrieve local port
	laddr := s.listenConn.Addr()
	s.listenAddr, err = net.ResolveTCPAddr(laddr.Network(), laddr.String())
	if err != nil {
		return fmt.Errorf("resolve TCPAddr error %v", err)
	}

	s.signals.stop = make(chan struct{})

	// start server routines
	s.wg.Add(1)
	go s.recvPacketRoutine()

	s.running.Store(true)

	fmt.Printf("TCP echo service started on %s:%d\n", ListenIp, ListenPort)
	return nil
}

func (s *TcpServer) Stop() {
	if !s.running.Load() {
		// already stopped, do nothing
		return
	}
	s.running.Store(false)

	close(s.signals.stop)
	s.listenConn.Close()
	s.wg.Wait()
}

func (s *TcpServer) recvPacketRoutine() {
	defer s.wg.Done()

	for {
		select {
		case <-s.signals.stop:
			return

		default:
		}

		s.listenConn.SetDeadline(time.Now().Add(5 * time.Second))
		tcpConn, err := s.listenConn.Accept()
		if err != nil {
			// accept failure
			continue
		}

		remoteAddr := tcpConn.RemoteAddr().(*net.TCPAddr)
		addrStr := remoteAddr.String()
		// allocate a new packet buffer for every read
		pkt := make([]byte, 4096)

		// tcp recv, blocking until packet arrives or conn.Close()
		tcpConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := tcpConn.Read(pkt[:])
		if err != nil {
			pkt = nil
			//log.Error("Read from UDP error: %v\n", err)
			if n == 0 {
				// listenConn closed
				return
			}
			continue
		}

		recvTime := time.Now()

		// add total recv bytes
		atomic.AddUint64(&s.stats.totalRecvBytes, uint64(n))

		//log.Trace("receive udp packet (%s -> %s): %+v", addrStr, s.listenAddr.String(), pkt.Packet)
		//log.Info("Receive [%s] packet (%s -> %s), %d bytes", msgType, addrStr, s.listenAddr.String(), n)

		s.remoteConnectionMapMutex.Lock()
		conn, found := s.remoteConnectionMap[addrStr]
		s.remoteConnectionMapMutex.Unlock()

		tuple := new(TupleData)
		tuple.msg = string(pkt)
		tuple.SrcIp = remoteAddr.IP.String()
		tuple.SrcPort = remoteAddr.Port
		tuple.DstIp = ListenIp
		tuple.DstPort = ListenPort
		tuple.Timestamp = recvTime

		if found {
			// existing connection
			atomic.StoreInt64(&conn.LastLocalRecvTime, recvTime.UnixMicro())
			conn.echoQueue <- tuple

		} else {
			// create new connection
			conn = &ConnData{
				InitTime:      recvTime.UnixMicro(),
				LocalAddr:     s.listenAddr,
				RemoteAddr:    remoteAddr,
				nativeConn:    tcpConn,
				connType:      TCP_CONN,
				idleTimeoutMs: CONNECTION_IDLE_TIMEOUTMS,
			}
			conn.echoQueue = make(chan *TupleData)
			// setup new routine for connection
			s.remoteConnectionMapMutex.Lock()
			s.remoteConnectionMap[addrStr] = conn
			s.remoteConnectionMapMutex.Unlock()

			conn.echoQueue <- tuple

			//log.Info("Accept new UDP connection from %s to %s", addrStr, s.listenAddr.String())

			// launch connection routine
			s.wg.Add(1)
			go s.connectionRoutine(conn)
		}
	}
}

func (s *TcpServer) connectionRoutine(conn *ConnData) {
	addrStr := conn.RemoteAddr.String()
	defer s.wg.Done()
	//defer log.Debug("Connection routine: %s stopped", addrStr)

	//log.Debug("Connection routine: %s started", addrStr)

	// stop receiving packets and clean up
	defer func() {
		// remove conn from remoteConnectionMap
		s.remoteConnectionMapMutex.Lock()
		delete(s.remoteConnectionMap, addrStr)
		s.remoteConnectionMapMutex.Unlock()
		conn.Close()
	}()

	for {
		select {
		case <-s.signals.stop:
			return

		case <-time.After(time.Duration(conn.idleTimeoutMs) * time.Millisecond):
			// timeout, quit routine
			//log.Debug("Connection routine idle timeout")
			return

		case tuple, ok := <-conn.echoQueue:
			if !ok {
				return
			}
			if tuple == nil {
				continue
			}
			//log.Debug("Received udp packet len [%d] from addr: %s\n", len(pkt.Packet), addrStr)
			conn.nativeConn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
			conn.nativeConn.Write([]byte(tuple.String()))
		}
	}
}
