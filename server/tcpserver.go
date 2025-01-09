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

		// incoming connection
		remoteAddr := tcpConn.RemoteAddr().(*net.TCPAddr)
		addrStr := remoteAddr.String()
		conn := &ConnData{
			InitTime:      time.Now().UnixMicro(),
			LocalAddr:     s.listenAddr,
			RemoteAddr:    remoteAddr,
			nativeConn:    tcpConn,
			connType:      TCP_CONN,
			idleTimeoutMs: CONNECTION_IDLE_TIMEOUTMS,
			echoQueue:     make(chan *TupleData, 4096),
		}
		// record new connection
		s.remoteConnectionMapMutex.Lock()
		s.remoteConnectionMap[addrStr] = conn
		s.remoteConnectionMapMutex.Unlock()

		// launch connection routine
		s.wg.Add(1)
		go s.connectionRoutine(conn)
	}
}

func (s *TcpServer) connectionRoutine(conn *ConnData) {
	defer s.wg.Done()
	// stop connection and clean up
	defer func() {
		// remove conn from remoteConnectionMap
		s.remoteConnectionMapMutex.Lock()
		delete(s.remoteConnectionMap, conn.RemoteAddr.String())
		s.remoteConnectionMapMutex.Unlock()
		conn.Close()
	}()

	s.wg.Add(1)
	go s.echoRoutine(conn)

	// allocate a common packet buffer for every read
	pkt := make([]byte, MESSAGE_DATA_LENGTH)

	for {
		select {
		case <-s.signals.stop:
			return
		default:
		}

		// tcp recv, blocking until packet arrives or conn.Close()
		conn.nativeConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := conn.nativeConn.Read(pkt[:])
		if err != nil {
			//log.Error("Read from TCP error: %v\n", err)
			if n == 0 {
				// tcpConn closed
				return
			}
			continue
		}

		// add total recv bytes
		recvTime := time.Now()
		atomic.AddUint64(&s.stats.totalRecvBytes, uint64(n))
		atomic.StoreInt64(&conn.LastLocalRecvTime, recvTime.UnixMicro())
		//log.Trace("receive udp packet (%s -> %s): %+v", addrStr, s.listenAddr.String(), pkt.Packet)
		//log.Info("Receive [%s] packet (%s -> %s), %d bytes", msgType, addrStr, s.listenAddr.String(), n)

		tuple := new(TupleData)
		tuple.echoData = make([]byte, n)
		copy(tuple.echoData, pkt[:n])
		tuple.SrcIp = conn.RemoteAddr.(*net.TCPAddr).IP.String()
		tuple.SrcPort = conn.RemoteAddr.(*net.TCPAddr).Port
		tuple.DstIp = ListenIp
		tuple.DstPort = ListenPort
		tuple.Timestamp = recvTime

		tuple.EchoId = atomic.AddInt64(&conn.echoId, 1)
		conn.echoQueue <- tuple
		//log.Info("Accept new UDP connection from %s to %s", addrStr, s.listenAddr.String())
	}
}

func (s *TcpServer) echoRoutine(conn *ConnData) {
	defer s.wg.Done()

	for {
		select {
		case <-s.signals.stop:
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
			conn.nativeConn.Write(tuple.Serialize())
			atomic.AddUint64(&s.stats.totalSendBytes, uint64(len(tuple.serializedStr)))
		}
	}
}
