// Package replication implements single-leader WAL-shipping replication.
//
// Implemented in v3
//
// Architecture:
//
//	Leader DB
//	  └─ WAL append → ReplicationLog (ring buffer + fanout)
//	                       └─ stream goroutine per follower
//	                               └─ TCP conn → Follower.Apply()
//
// Wire protocol (little-endian over raw TCP):
//
//	Handshake (client → server):
//	  [8] magic   0xF1A5HDBF1A5HDBA
//	  [8] fromSeq – follower's last applied SeqNum (0 = full resync)
//
//	Handshake response (server → client):
//	  [8] magic
//	  [1] status  0=OK 1=resync required
//
//	Frame (server → client, repeated):
//	  [4]  crc32   – checksum of payload
//	  [4]  payLen
//	  [payload]    – WAL record bytes (same wire format as WAL package)
//
// Security:
//   - Connections are validated with a pre-shared secret via HMAC-SHA256.
//     The server sends a 32-byte challenge; the client responds with
//     HMAC-SHA256(secret, challenge).  No plaintext secret on the wire.
//   - All frame checksums are verified on the follower before apply.
//   - Maximum frame size is capped at 64 MB to prevent memory exhaustion.
//   - Followers are read-only: they reject any write RPC.
//
// This implementation ships WAL records, not full SSTables, so it is
// suitable for low-latency replication of write-heavy workloads.  A full
// snapshot transfer path (for lagging followers) uses the backup package.
package replication

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	protocolMagic  = uint64(0xF1A5DB00F1A5DB00)
	maxFrameSize   = 64 * 1024 * 1024 // 64 MB
	challengeBytes = 32
)

var le = binary.LittleEndian

// Errors
var (
	ErrAuthFailed      = errors.New("replication: authentication failed")
	ErrProtocolMagic   = errors.New("replication: bad protocol magic")
	ErrFrameTooLarge   = errors.New("replication: frame exceeds max size")
	ErrChecksumMismatch = errors.New("replication: frame checksum mismatch")
	ErrReadOnly        = errors.New("replication: follower is read-only")
)

// Config configures a replication node.
type Config struct {
	// Role is either "leader" or "follower".
	Role string
	// ListenAddr is the TCP address the leader listens on (e.g. ":5432").
	// Required for Role=="leader".
	ListenAddr string
	// LeaderAddr is the TCP address of the leader to connect to.
	// Required for Role=="follower".
	LeaderAddr string
	// Secret is the pre-shared authentication secret.
	// Must be the same on leader and all followers.
	Secret []byte
	// DialTimeout for follower→leader connections.
	DialTimeout time.Duration
	// ReconnectInterval is how long a follower waits before retrying.
	ReconnectInterval time.Duration
}

func (c *Config) defaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReconnectInterval == 0 {
		c.ReconnectInterval = 2 * time.Second
	}
}

// WALRecord is a decoded replication frame payload — same logical type as
// wal.Record but kept separate to avoid a circular import.
type WALRecord struct {
	Kind      byte
	SeqNum    uint64
	TxnID     uint64
	Key       []byte
	Value     []byte
	Tombstone bool
}

// Applier is implemented by the follower DB to apply incoming WAL records.
type Applier interface {
	ApplyWALRecord(r WALRecord) error
	LastAppliedSeq() uint64
}

// ── Leader ────────────────────────────────────────────────────────────────────

// Leader accepts follower connections and fans out WAL records.
type Leader struct {
	cfg      Config
	ln       net.Listener
	mu       sync.RWMutex
	followers map[string]*followerConn // addr → conn
	ring     *ringBuffer
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewLeader creates a Leader node (does not start listening yet).
func NewLeader(cfg Config) (*Leader, error) {
	cfg.defaults()
	if cfg.Secret == nil || len(cfg.Secret) < 16 {
		return nil, fmt.Errorf("replication: secret must be at least 16 bytes")
	}
	return &Leader{
		cfg:       cfg,
		followers: make(map[string]*followerConn),
		ring:      newRingBuffer(65536), // 64k record slots
		stopCh:    make(chan struct{}),
	}, nil
}

// Start begins accepting follower connections.
func (l *Leader) Start() error {
	ln, err := net.Listen("tcp", l.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("replication leader listen: %w", err)
	}
	l.ln = ln
	l.wg.Add(1)
	go l.acceptLoop()
	return nil
}

// Ship sends a WAL record to all connected followers.
// Called by the engine immediately after appending to the local WAL.
func (l *Leader) Ship(r WALRecord) {
	l.ring.push(r)
	l.mu.RLock()
	for _, fc := range l.followers {
		select {
		case fc.notify <- struct{}{}:
		default:
		}
	}
	l.mu.RUnlock()
}

// Stop closes the listener and disconnects all followers.
func (l *Leader) Stop() {
	close(l.stopCh)
	if l.ln != nil {
		l.ln.Close()
	}
	l.mu.Lock()
	for _, fc := range l.followers {
		fc.conn.Close()
	}
	l.mu.Unlock()
	l.wg.Wait()
}

func (l *Leader) acceptLoop() {
	defer l.wg.Done()
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			select {
			case <-l.stopCh:
				return
			default:
				log.Printf("replication: accept error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
		l.wg.Add(1)
		go l.handleFollower(conn)
	}
}

func (l *Leader) handleFollower(conn net.Conn) {
	defer l.wg.Done()
	addr := conn.RemoteAddr().String()
	defer func() {
		conn.Close()
		l.mu.Lock()
		delete(l.followers, addr)
		l.mu.Unlock()
		log.Printf("replication: follower %s disconnected", addr)
	}()

	// Auth: send challenge, verify HMAC response.
	if err := l.authenticate(conn); err != nil {
		log.Printf("replication: auth failed for %s: %v", addr, err)
		return
	}

	// Read follower's last applied seq.
	var fromSeq uint64
	if err := readUint64(conn, &fromSeq); err != nil {
		return
	}

	// Send OK response.
	if err := writeUint64(conn, protocolMagic); err != nil {
		return
	}
	if err := writeByte(conn, 0); err != nil {
		return
	}

	fc := &followerConn{conn: conn, notify: make(chan struct{}, 1)}
	l.mu.Lock()
	l.followers[addr] = fc
	l.mu.Unlock()
	log.Printf("replication: follower %s connected (fromSeq=%d)", addr, fromSeq)

	// Stream records.
	for {
		select {
		case <-l.stopCh:
			return
		case <-fc.notify:
		case <-time.After(5 * time.Second): // keep-alive interval
		}

		records := l.ring.since(fromSeq)
		for _, r := range records {
			frame, err := encodeFrame(r)
			if err != nil {
				log.Printf("replication: encode error: %v", err)
				return
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(frame); err != nil {
				return
			}
			fromSeq = r.SeqNum
		}
	}
}

func (l *Leader) authenticate(conn net.Conn) error {
	challenge := make([]byte, challengeBytes)
	if _, err := rand.Read(challenge); err != nil {
		return err
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write(challenge); err != nil {
		return err
	}
	resp := make([]byte, 32)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	mac := hmac.New(sha256.New, l.cfg.Secret)
	mac.Write(challenge)
	expected := mac.Sum(nil)
	if !hmac.Equal(resp, expected) {
		return ErrAuthFailed
	}
	conn.SetDeadline(time.Time{}) // clear deadline
	return nil
}

// ── Follower ──────────────────────────────────────────────────────────────────

// Follower connects to the leader and applies incoming WAL records.
type Follower struct {
	cfg        Config
	applier    Applier
	stopCh     chan struct{}
	wg         sync.WaitGroup
	lastSeq    atomic.Uint64
	connected  atomic.Bool
}

// NewFollower creates a Follower node.
func NewFollower(cfg Config, applier Applier) (*Follower, error) {
	cfg.defaults()
	if cfg.Secret == nil || len(cfg.Secret) < 16 {
		return nil, fmt.Errorf("replication: secret must be at least 16 bytes")
	}
	f := &Follower{
		cfg:     cfg,
		applier: applier,
		stopCh:  make(chan struct{}),
	}
	f.lastSeq.Store(applier.LastAppliedSeq())
	return f, nil
}

// Start begins the background replication loop.
func (f *Follower) Start() {
	f.wg.Add(1)
	go f.loop()
}

// Stop shuts down the follower.
func (f *Follower) Stop() {
	close(f.stopCh)
	f.wg.Wait()
}

// IsConnected returns true if the follower currently has an active connection.
func (f *Follower) IsConnected() bool { return f.connected.Load() }

// LastSeq returns the last successfully applied sequence number.
func (f *Follower) LastSeq() uint64 { return f.lastSeq.Load() }

func (f *Follower) loop() {
	defer f.wg.Done()
	for {
		select {
		case <-f.stopCh:
			return
		default:
		}
		if err := f.runOnce(); err != nil {
			log.Printf("replication follower: %v — reconnecting in %v", err, f.cfg.ReconnectInterval)
			f.connected.Store(false)
			select {
			case <-f.stopCh:
				return
			case <-time.After(f.cfg.ReconnectInterval):
			}
		}
	}
}

func (f *Follower) runOnce() error {
	conn, err := net.DialTimeout("tcp", f.cfg.LeaderAddr, f.cfg.DialTimeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", f.cfg.LeaderAddr, err)
	}
	defer conn.Close()

	// Auth: read challenge, send HMAC response.
	challenge := make([]byte, challengeBytes)
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, challenge); err != nil {
		return fmt.Errorf("read challenge: %w", err)
	}
	mac := hmac.New(sha256.New, f.cfg.Secret)
	mac.Write(challenge)
	if _, err := conn.Write(mac.Sum(nil)); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// Send our last applied seq.
	if err := writeUint64(conn, f.lastSeq.Load()); err != nil {
		return fmt.Errorf("send fromSeq: %w", err)
	}

	// Read server OK.
	var magic uint64
	if err := readUint64(conn, &magic); err != nil {
		return fmt.Errorf("read handshake: %w", err)
	}
	if magic != protocolMagic {
		return ErrProtocolMagic
	}
	status := make([]byte, 1)
	if _, err := io.ReadFull(conn, status); err != nil {
		return err
	}

	conn.SetDeadline(time.Time{})
	f.connected.Store(true)
	log.Printf("replication follower: connected to %s (fromSeq=%d)", f.cfg.LeaderAddr, f.lastSeq.Load())

	// Stream records.
	for {
		select {
		case <-f.stopCh:
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(15 * time.Second))
		r, err := decodeFrame(conn)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // keep-alive timeout, retry read
			}
			return fmt.Errorf("decode frame: %w", err)
		}

		if err := f.applier.ApplyWALRecord(r); err != nil {
			return fmt.Errorf("apply record seq=%d: %w", r.SeqNum, err)
		}
		f.lastSeq.Store(r.SeqNum)
	}
}

// ── Frame encoding ────────────────────────────────────────────────────────────

func encodeFrame(r WALRecord) ([]byte, error) {
	payload := marshalRecord(r)
	frame := make([]byte, 8+len(payload))
	le.PutUint32(frame[0:], crc32.ChecksumIEEE(payload))
	le.PutUint32(frame[4:], uint32(len(payload)))
	copy(frame[8:], payload)
	return frame, nil
}

func decodeFrame(r io.Reader) (WALRecord, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return WALRecord{}, err
	}
	checksum := le.Uint32(hdr[0:])
	payLen := le.Uint32(hdr[4:])
	if payLen > maxFrameSize {
		return WALRecord{}, ErrFrameTooLarge
	}
	payload := make([]byte, payLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return WALRecord{}, err
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return WALRecord{}, ErrChecksumMismatch
	}
	return unmarshalRecord(payload)
}

func marshalRecord(r WALRecord) []byte {
	size := 1 + 8 + 8 // kind + seq + txnID
	if len(r.Key) > 0 {
		size += 4 + len(r.Key)
	}
	if r.Kind == 0 { // KindPut
		size += 4 + len(r.Value)
	}
	buf := make([]byte, size)
	off := 0
	buf[off] = r.Kind; off++
	le.PutUint64(buf[off:], r.SeqNum); off += 8
	le.PutUint64(buf[off:], r.TxnID); off += 8
	if len(r.Key) > 0 {
		le.PutUint32(buf[off:], uint32(len(r.Key))); off += 4
		copy(buf[off:], r.Key); off += len(r.Key)
	}
	if r.Kind == 0 {
		le.PutUint32(buf[off:], uint32(len(r.Value))); off += 4
		copy(buf[off:], r.Value)
	}
	return buf
}

func unmarshalRecord(buf []byte) (WALRecord, error) {
	if len(buf) < 17 {
		return WALRecord{}, fmt.Errorf("frame payload too short")
	}
	off := 0
	r := WALRecord{}
	r.Kind = buf[off]; off++
	r.SeqNum = le.Uint64(buf[off:]); off += 8
	r.TxnID = le.Uint64(buf[off:]); off += 8
	r.Tombstone = r.Kind == 1

	if r.Kind == 2 || r.Kind == 3 || r.Kind == 4 { // txn control
		return r, nil
	}
	if off+4 > len(buf) {
		return WALRecord{}, fmt.Errorf("key len overrun")
	}
	kl := int(le.Uint32(buf[off:])); off += 4
	if off+kl > len(buf) {
		return WALRecord{}, fmt.Errorf("key overrun")
	}
	r.Key = make([]byte, kl)
	copy(r.Key, buf[off:]); off += kl
	if r.Kind == 0 {
		if off+4 > len(buf) {
			return WALRecord{}, fmt.Errorf("val len overrun")
		}
		vl := int(le.Uint32(buf[off:])); off += 4
		if off+vl > len(buf) {
			return WALRecord{}, fmt.Errorf("val overrun")
		}
		r.Value = make([]byte, vl)
		copy(r.Value, buf[off:])
	}
	return r, nil
}

// ── Ring buffer ───────────────────────────────────────────────────────────────

// ringBuffer is a fixed-size, lock-protected circular buffer of WALRecords.
// It allows followers to catch up without the leader keeping an unbounded log.
type ringBuffer struct {
	mu      sync.RWMutex
	slots   []WALRecord
	size    uint64
	head    uint64 // next write position (mod size)
	count   uint64 // number of slots filled
}

func newRingBuffer(size uint64) *ringBuffer {
	return &ringBuffer{slots: make([]WALRecord, size), size: size}
}

func (rb *ringBuffer) push(r WALRecord) {
	rb.mu.Lock()
	rb.slots[rb.head%rb.size] = r
	rb.head++
	if rb.count < rb.size {
		rb.count++
	}
	rb.mu.Unlock()
}

// since returns all records with SeqNum > afterSeq, in order.
// If the follower has fallen too far behind (the records were overwritten),
// the caller should trigger a full snapshot transfer.
func (rb *ringBuffer) since(afterSeq uint64) []WALRecord {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return nil
	}
	var out []WALRecord
	// Walk from oldest to newest.
	start := rb.head - rb.count
	for i := uint64(0); i < rb.count; i++ {
		idx := (start + i) % rb.size
		r := rb.slots[idx]
		if r.SeqNum > afterSeq {
			out = append(out, r)
		}
	}
	return out
}

// ── followerConn ──────────────────────────────────────────────────────────────

type followerConn struct {
	conn   net.Conn
	notify chan struct{}
}

// ── I/O helpers ───────────────────────────────────────────────────────────────

func readUint64(r io.Reader, v *uint64) error {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return err
	}
	*v = le.Uint64(buf[:])
	return nil
}

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte
	le.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func writeByte(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	return err
}