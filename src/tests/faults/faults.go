// Package faults provides a fault-injection filesystem for chaos testing.
//
// FaultFS wraps any backend.FS and injects configurable faults:
// I/O errors, latency, and byte corruption. Use it to verify the
// engine handles disk failures gracefully.

package faults

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/Ari-Ghosh/flash-db/src/backend"
)

// Config controls which faults to inject.
type Config struct {
	// IOFailureRate is the probability (0.0-1.0) of an I/O error.
	IOFailureRate float64
	// SlowIOSleep injects a fixed delay before every I/O operation.
	SlowIOSleep time.Duration
	// CorruptWriteRate is the probability (0.0-1.0) of corrupting
	// a byte during a write operation.
	CorruptWriteRate float64
	// SyncFailureRate is the probability (0.0-1.0) of Sync() failing.
	SyncFailureRate float64
	// ReadErrorRate is the probability (0.0-1.0) of Read/ReadAt failing.
	ReadErrorRate float64
}

// FaultFS wraps a real FS and injects faults per the Config.
type FaultFS struct {
	inner backend.FS
	cfg   Config
	rng   *rand.Rand
}

// New creates a FaultFS wrapping the given backend.
func New(inner backend.FS, cfg Config) *FaultFS {
	return &FaultFS{
		inner: inner,
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (f *FaultFS) maybeFail() error {
	if f.rng.Float64() < f.cfg.IOFailureRate {
		return fmt.Errorf("faultfs: injected I/O failure")
	}
	return nil
}

func (f *FaultFS) OpenRead(path string) (backend.File, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	fi, err := f.inner.OpenRead(path)
	if err != nil {
		return nil, err
	}
	return &faultFile{inner: fi, cfg: f.cfg, rng: f.rng}, nil
}

func (f *FaultFS) Create(path string) (backend.File, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	fi, err := f.inner.Create(path)
	if err != nil {
		return nil, err
	}
	return &faultFile{inner: fi, cfg: f.cfg, rng: f.rng}, nil
}

func (f *FaultFS) OpenReadWrite(path string) (backend.File, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	fi, err := f.inner.OpenReadWrite(path)
	if err != nil {
		return nil, err
	}
	return &faultFile{inner: fi, cfg: f.cfg, rng: f.rng}, nil
}

func (f *FaultFS) OpenAppend(path string) (backend.File, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	fi, err := f.inner.OpenAppend(path)
	if err != nil {
		return nil, err
	}
	return &faultFile{inner: fi, cfg: f.cfg, rng: f.rng}, nil
}

func (f *FaultFS) Remove(path string) error {
	if err := f.maybeFail(); err != nil {
		return err
	}
	return f.inner.Remove(path)
}

func (f *FaultFS) MkdirAll(path string, perm os.FileMode) error {
	if err := f.maybeFail(); err != nil {
		return err
	}
	return f.inner.MkdirAll(path, perm)
}

func (f *FaultFS) Rename(old, newname string) error {
	if err := f.maybeFail(); err != nil {
		return err
	}
	return f.inner.Rename(old, newname)
}

func (f *FaultFS) List(pattern string) ([]string, error) {
	if err := f.maybeFail(); err != nil {
		return nil, err
	}
	return f.inner.List(pattern)
}

// faultFile wraps a backend.File to inject faults on reads, writes, and syncs.
type faultFile struct {
	inner backend.File
	cfg   Config
	rng   *rand.Rand
}

func (ff *faultFile) Read(p []byte) (int, error) {
	if ff.rng.Float64() < ff.cfg.ReadErrorRate {
		return 0, fmt.Errorf("faultfs: injected read error")
	}
	return ff.inner.Read(p)
}

func (ff *faultFile) Write(p []byte) (int, error) {
	ff.corrupt(p)
	return ff.inner.Write(p)
}

func (ff *faultFile) ReadAt(p []byte, off int64) (int, error) {
	if ff.rng.Float64() < ff.cfg.ReadErrorRate {
		return 0, fmt.Errorf("faultfs: injected read error")
	}
	return ff.inner.ReadAt(p, off)
}

func (ff *faultFile) WriteAt(p []byte, off int64) (int, error) {
	ff.corrupt(p)
	return ff.inner.WriteAt(p, off)
}

func (ff *faultFile) Seek(offset int64, whence int) (int64, error) {
	return ff.inner.Seek(offset, whence)
}

func (ff *faultFile) Close() error {
	return ff.inner.Close()
}

func (ff *faultFile) Sync() error {
	if ff.rng.Float64() < ff.cfg.SyncFailureRate {
		return fmt.Errorf("faultfs: injected sync failure")
	}
	return ff.inner.Sync()
}

func (ff *faultFile) Stat() (os.FileInfo, error) {
	return ff.inner.Stat()
}

func (ff *faultFile) corrupt(p []byte) {
	if ff.cfg.CorruptWriteRate > 0 && len(p) > 0 {
		for i := range p {
			if ff.rng.Float64() < ff.cfg.CorruptWriteRate {
				p[i] ^= 0xFF
			}
		}
	}
}

// Ensure FaultFS implements FS at compile time.
var _ backend.FS = (*FaultFS)(nil)

// Ensure faultFile implements File at compile time.
var _ backend.File = (*faultFile)(nil)
