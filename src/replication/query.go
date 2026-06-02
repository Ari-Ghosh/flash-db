// Package replication — query protocol for distributed fan-out.
//
// The leader sends queries to followers on the existing replication TCP
// connection.  Since the WAL streaming goroutine also writes to the same
// connection, a write mutex serialises all writes.
//
// Wire format (little-endian):
//
//	QueryRequest (leader → follower):
//	  [1] kind = 0x02
//	  [1] reverse
//	  [8] snapSeq
//	  [1] includeTombstones
//	  [4] lowerLen
//	  [lowerLen] lower
//	  [4] upperLen
//	  [upperLen] upper
//	  [4] prefixLen
//	  [prefixLen] prefix
//
//	QueryResponse (follower → leader):
//	  [1] kind = 0x03
//	  [1] tombstones
//	  [8] seqNum
//	  [4] keyLen
//	  [keyLen] key
//	  [4] valLen
//	  [valLen] value
//
//	QueryDone (follower → leader):
//	  [1] kind = 0x04

package replication

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	frameKindQueryReq  byte = 0x02
	frameKindQueryResp byte = 0x03
	frameKindQueryDone byte = 0x04
)

// QueryRequest describes a fan-out query.
type QueryRequest struct {
	Reverse           bool
	SnapshotSeq       uint64
	IncludeTombstones bool
	LowerBound        []byte
	UpperBound        []byte
	Prefix            []byte
}

// QueryResponse is one key-value result from a follower.
type QueryResponse struct {
	Tombstone bool
	SeqNum    uint64
	Key       []byte
	Value     []byte
}

// AppendQueryApplier is the interface the follower DB must implement for
// distributed query support.
type AppendQueryApplier interface {
	Applier
	ExecuteQuery(req QueryRequest) (QueryResultIter, error)
}

// QueryResultIter abstracts a local query result.
type QueryResultIter interface {
	Next() (QueryResponse, bool)
	Close() error
}

func encodeQueryRequest(req QueryRequest) []byte {
	size := 1 + 1 + 8 + 1 + 4 + len(req.LowerBound) + 4 + len(req.UpperBound) + 4 + len(req.Prefix)
	buf := make([]byte, size)
	off := 0
	buf[off] = frameKindQueryReq
	off++
	if req.Reverse {
		buf[off] = 1
	}
	off++
	binary.LittleEndian.PutUint64(buf[off:], req.SnapshotSeq)
	off += 8
	if req.IncludeTombstones {
		buf[off] = 1
	}
	off++
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(req.LowerBound)))
	off += 4
	copy(buf[off:], req.LowerBound)
	off += len(req.LowerBound)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(req.UpperBound)))
	off += 4
	copy(buf[off:], req.UpperBound)
	off += len(req.UpperBound)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(req.Prefix)))
	off += 4
	copy(buf[off:], req.Prefix)
	return buf
}

func decodeQueryRequest(r io.Reader) (QueryRequest, error) {
	var req QueryRequest
	var revB [1]byte
	if _, err := io.ReadFull(r, revB[:]); err != nil {
		return req, err
	}
	req.Reverse = revB[0] != 0
	var seqB [8]byte
	if _, err := io.ReadFull(r, seqB[:]); err != nil {
		return req, err
	}
	req.SnapshotSeq = binary.LittleEndian.Uint64(seqB[:])
	var tombB [1]byte
	if _, err := io.ReadFull(r, tombB[:]); err != nil {
		return req, err
	}
	req.IncludeTombstones = tombB[0] != 0

	var lenB [4]byte
	if _, err := io.ReadFull(r, lenB[:]); err != nil {
		return req, err
	}
	lowerLen := int(binary.LittleEndian.Uint32(lenB[:]))
	if lowerLen > 0 {
		req.LowerBound = make([]byte, lowerLen)
		if _, err := io.ReadFull(r, req.LowerBound); err != nil {
			return req, err
		}
	}
	if _, err := io.ReadFull(r, lenB[:]); err != nil {
		return req, err
	}
	upperLen := int(binary.LittleEndian.Uint32(lenB[:]))
	if upperLen > 0 {
		req.UpperBound = make([]byte, upperLen)
		if _, err := io.ReadFull(r, req.UpperBound); err != nil {
			return req, err
		}
	}
	if _, err := io.ReadFull(r, lenB[:]); err != nil {
		return req, err
	}
	prefixLen := int(binary.LittleEndian.Uint32(lenB[:]))
	if prefixLen > 0 {
		req.Prefix = make([]byte, prefixLen)
		if _, err := io.ReadFull(r, req.Prefix); err != nil {
			return req, err
		}
	}
	return req, nil
}

func encodeQueryResponse(resp QueryResponse) []byte {
	size := 1 + 1 + 8 + 4 + len(resp.Key) + 4 + len(resp.Value)
	buf := make([]byte, size)
	off := 0
	buf[off] = frameKindQueryResp
	off++
	if resp.Tombstone {
		buf[off] = 1
	}
	off++
	binary.LittleEndian.PutUint64(buf[off:], resp.SeqNum)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(resp.Key)))
	off += 4
	copy(buf[off:], resp.Key)
	off += len(resp.Key)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(resp.Value)))
	off += 4
	copy(buf[off:], resp.Value)
	return buf
}

func decodeQueryResponse(r io.Reader) (QueryResponse, error) {
	var resp QueryResponse
	var tombB [1]byte
	if _, err := io.ReadFull(r, tombB[:]); err != nil {
		return resp, err
	}
	resp.Tombstone = tombB[0] != 0
	var seqB [8]byte
	if _, err := io.ReadFull(r, seqB[:]); err != nil {
		return resp, err
	}
	resp.SeqNum = binary.LittleEndian.Uint64(seqB[:])
	var lenB [4]byte
	if _, err := io.ReadFull(r, lenB[:]); err != nil {
		return resp, err
	}
	keyLen := int(binary.LittleEndian.Uint32(lenB[:]))
	if keyLen > 0 {
		resp.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(r, resp.Key); err != nil {
			return resp, err
		}
	}
	if _, err := io.ReadFull(r, lenB[:]); err != nil {
		return resp, err
	}
	valLen := int(binary.LittleEndian.Uint32(lenB[:]))
	if valLen > 0 {
		resp.Value = make([]byte, valLen)
		if _, err := io.ReadFull(r, resp.Value); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func encodeQueryDone(count uint32) []byte {
	buf := make([]byte, 1+4)
	buf[0] = frameKindQueryDone
	binary.LittleEndian.PutUint32(buf[1:], count)
	return buf
}

// ── leader-side fan-out ──────────────────────────────────────────────────────

// FanOutQuery sends a query to all connected followers and returns per-follower
// result channels.  Each query is sent on the follower's existing WAL connection
// using the connection's write mutex for safe multiplexing.
func (l *Leader) FanOutQuery(req QueryRequest) []<-chan QueryResponse {
	l.mu.RLock()
	defer l.mu.RUnlock()

	streams := make([]<-chan QueryResponse, 0, len(l.followers))
	var wg sync.WaitGroup

	for _, fc := range l.followers {
		ch := make(chan QueryResponse, 64)
		streams = append(streams, ch)
		wg.Add(1)
		go func(fconn *followerConn, results chan<- QueryResponse) {
			defer wg.Done()
			defer close(results)

			body := encodeQueryRequest(req)
			fconn.writeMu.Lock()
			if _, err := fconn.conn.Write(body); err != nil {
				fconn.writeMu.Unlock()
				return
			}
			fconn.writeMu.Unlock()

			for {
				kind := make([]byte, 1)
				if _, err := io.ReadFull(fconn.conn, kind); err != nil {
					return
				}
				switch kind[0] {
				case frameKindQueryResp:
					resp, err := decodeQueryResponse(fconn.conn)
					if err != nil {
						return
					}
					results <- resp
				case frameKindQueryDone:
					return
				default:
					return
				}
			}
		}(fc, ch)
	}
	return streams
}

// ── follower-side query responder ──────────────────────────────────────────

// serveQuery handles an incoming query on the follower's replication connection.
// The caller has already consumed the frameKindQueryReq byte from the stream.
// Writes QueryResponse/QueryDone frames back to the same conn.
func serveQuery(conn io.ReadWriter, qa AppendQueryApplier) error {
	req, err := decodeQueryRequest(conn)
	if err != nil {
		return fmt.Errorf("decode query: %w", err)
	}
	iter, err := qa.ExecuteQuery(req)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer func() { _ = iter.Close() }()

	count := uint32(0)
	for {
		resp, ok := iter.Next()
		if !ok {
			break
		}
		body := encodeQueryResponse(resp)
		if _, err := conn.Write(body); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
		count++
	}
	if _, err := conn.Write(encodeQueryDone(count)); err != nil {
		return fmt.Errorf("write done: %w", err)
	}
	return nil
}
