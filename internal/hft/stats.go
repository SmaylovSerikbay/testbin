package hft

import (
	"sync"
)

// SessionStats — PnL и win rate по user stream (reduce-only).
type SessionStats struct {
	mu            sync.Mutex
	totalRealized float64
	wins          int64
	losses        int64
	impulses      []string
	impulseCap    int
	lastOrderErr  string
}

func NewSessionStats(impulseCap int) *SessionStats {
	if impulseCap <= 0 {
		impulseCap = 12
	}
	return &SessionStats{impulseCap: impulseCap}
}

func (s *SessionStats) AddRealized(rp float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalRealized += rp
	if rp > 1e-8 {
		s.wins++
	} else if rp < -1e-8 {
		s.losses++
	}
}

func (s *SessionStats) SetOrderError(msg string) {
	if msg == "" {
		return
	}
	s.mu.Lock()
	if len(msg) > 220 {
		msg = msg[:220] + "…"
	}
	s.lastOrderErr = msg
	s.mu.Unlock()
}

func (s *SessionStats) LastOrderError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastOrderErr
}

func (s *SessionStats) PushImpulse(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.impulses = append(s.impulses, line)
	if len(s.impulses) > s.impulseCap {
		s.impulses = s.impulses[len(s.impulses)-s.impulseCap:]
	}
}

func (s *SessionStats) Snapshot() (total float64, winRate float64, impulses []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	total = s.totalRealized
	if s.wins+s.losses > 0 {
		winRate = float64(s.wins) / float64(s.wins+s.losses) * 100
	}
	impulses = append([]string(nil), s.impulses...)
	return total, winRate, impulses
}
