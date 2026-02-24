package queue

import (
	"context"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

type WaitingMember struct {
	Rank  int64
	Score float64 // unix nano
}

type WaitingQueueSnapshot struct {
	Members   map[int64]WaitingMember
	FetchedAt time.Time
	Mu        *xsync.RBMutex
}

func NewWaitingQueueSnapshot() *WaitingQueueSnapshot {
	return &WaitingQueueSnapshot{
		Members:   make(map[int64]WaitingMember),
		FetchedAt: time.Now(),
		Mu:        xsync.NewRBMutex(),
	}
}

func (s *WaitingQueueSnapshot) isTier1(ctx context.Context, user_id int64) bool {
	// Tier1: Redis에 직접 확인
	// Tier2: Snapshot에서 확인
	rank, ok := s.GetRank(ctx, user_id)
	if !ok {
		return false // 어차피 다시 읽어야 함
	}
	return s.Size() <= 100 || rank <= int64(float64(s.Size())*0.01) // 100명 이하이거나 상위 1%
}

func (s *WaitingQueueSnapshot) GetRank(ctx context.Context, user_id int64) (rank int64, ok bool) {
	rtkn := s.Mu.RLock()
	defer s.Mu.RUnlock(rtkn)

	wmem, ok := s.Members[user_id]
	if ok {
		return wmem.Rank, true
	}
	return -1, false
}

func (s *WaitingQueueSnapshot) Size() int {
	return len(s.Members)
}

func (s *WaitingQueueSnapshot) Delete(user_id int64) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	delete(s.Members, user_id)
}

func (s *WaitingQueueSnapshot) Add(user_id int64, rank int64, score float64) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	m, ok := s.Members[user_id]
	if !ok {
		m = WaitingMember{}
	}
	m.Rank = rank
	m.Score = score
	s.Members[user_id] = m
}
