package gossip

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"SDCC_gossiping/detector"
)

// MemberInfo tiene traccia dello stato di un peer.
type MemberInfo struct {
	ID        string    `json:"id"`
	Addr      string    `json:"addr"`
	Heartbeat uint64    `json:"heartbeat"`
	LastSeen  time.Time `json:"-"`
	Alive     bool      `json:"alive"`
}

// Node rappresenta un membro del cluster gossip.
type Node struct {
	ID       string
	Addr     *net.UDPAddr
	peers    []*net.UDPAddr
	members  map[string]*MemberInfo
	mu       sync.RWMutex
	conn     *net.UDPConn
	tick     time.Duration
	detector *detector.FailureDetector
	inbox    chan []byte
}

// Config crea un Node configurato, senza connessione aperta.
func Config(id, bindAddr string, tick time.Duration, peerAddrs []string) (*Node, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", bindAddr)
	if err != nil {
		return nil, err
	}
	peers := make([]*net.UDPAddr, 0, len(peerAddrs))
	for _, p := range peerAddrs {
		if p == bindAddr {
			continue
		}
		pa, err := net.ResolveUDPAddr("udp", p)
		if err != nil {
			return nil, err
		}
		peers = append(peers, pa)
	}
	members := make(map[string]*MemberInfo)
	members[id] = &MemberInfo{ID: id, Addr: bindAddr, Heartbeat: 0, Alive: true}

	// Inizializza il FailureDetector (windowSize=5, thresholdPhi=8.0)
	fd := detector.New(5, 8.0)

	return &Node{
		ID:       id,
		Addr:     udpAddr,
		peers:    peers,
		members:  members,
		tick:     tick,
		detector: fd,
		inbox:    make(chan []byte, 100),
	}, nil
}

// Start apre la connessione UDP e lancia listener e ticker.
func (n *Node) Start() error {
	conn, err := net.ListenUDP("udp", n.Addr)
	if err != nil {
		return err
	}
	n.conn = conn

	go n.listen()
	go n.gossipLoop()

	return nil
}

// listen riceve pacchetti, aggiorna la view e li invia sull'inbox.
func (n *Node) listen() {
	buf := make([]byte, 4096)
	for {
		nr, addr, err := n.conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println("listen error:", err)
			continue
		}
		// copia del dato grezzo per invitare sul canale
		data := make([]byte, nr)
		copy(data, buf[:nr])
		n.inbox <- data

		var incoming map[string]MemberInfo
		if err := json.Unmarshal(data, &incoming); err != nil {
			fmt.Println("json unmarshal error:", err)
			continue
		}

		n.mu.Lock()
		now := time.Now()
		for id, info := range incoming {
			// notifica il detector del heartbeat ricevuto
			n.detector.HeartbeatNotify(id, now)

			mi, exists := n.members[id]
			if !exists || info.Heartbeat > mi.Heartbeat {
				info.LastSeen = now
				info.Alive = true
				n.members[id] = &MemberInfo{
					ID:        info.ID,
					Addr:      info.Addr,
					Heartbeat: info.Heartbeat,
					LastSeen:  info.LastSeen,
					Alive:     info.Alive,
				}
				fmt.Printf("[%s] updated member %s: hb=%d from %s\n",
					n.ID, id, info.Heartbeat, addr)
			}
		}
		n.mu.Unlock()
	}
}

// gossipLoop invia periodicamente il proprio stato a un peer casuale.
func (n *Node) gossipLoop() {
	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		now := time.Now()
		// incremento del mio heartbeat e notifico il detector
		my := n.members[n.ID]
		my.Heartbeat++
		my.LastSeen = now
		n.detector.HeartbeatNotify(n.ID, now)

		// preparo il payload gossip
		payload := make(map[string]MemberInfo, len(n.members))
		for id, mi := range n.members {
			payload[id] = *mi
		}
		data, err := json.Marshal(payload)
		if err != nil {
			fmt.Println("json marshal error:", err)
			n.mu.Unlock()
			continue
		}

		// seleziono un peer random e invio
		if len(n.peers) > 0 {
			idx := rand.Intn(len(n.peers))
			peer := n.peers[idx]
			if _, err := n.conn.WriteToUDP(data, peer); err != nil {
				fmt.Println("write error:", err)
			} else {
				fmt.Printf("[%s] sent gossip to %s\n", n.ID, peer)
			}
		}
		n.mu.Unlock()
	}
}

// NextGossipMessage espone il canale da cui leggere i raw gossip ricevuti.
func (n *Node) NextGossipMessage() <-chan []byte {
	return n.inbox
}

// SendToRandomPeer invia dati grezzi via gossip come payload custom.
func (n *Node) SendToRandomPeer(data []byte) error {
	if len(n.peers) == 0 {
		return nil
	}
	idx := rand.Intn(len(n.peers))
	peer := n.peers[idx]
	_, err := n.conn.WriteToUDP(data, peer)
	return err
}
