package queue

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/redis/go-redis/v9"
)

type WaitingQueueRepository struct {
	client    *redis.Client
	Snapshots *xsync.Map[string, *WaitingQueueSnapshot]
}

func NewWaitingQueueRepository() *WaitingQueueRepository {
	return &WaitingQueueRepository{
		client: redis.NewClient(&redis.Options{
			Addr: "redis-service:6379", // 같은 클러스터 내에 있으므로 Service 이름으로 호스트 지정가능
			// Password: "",
			// DB: 0, // default DB
		}),
		Snapshots: xsync.NewMap[string, *WaitingQueueSnapshot](),
		// key: "waiting:" + event_id,
	}
}
func (r *WaitingQueueRepository) Close() { r.client.Close() }

func (r *WaitingQueueRepository) Add(ctx context.Context, event_id string, user_id int64) (rank int64, err error) {
	score, err := r.enqueue(ctx, event_id, user_id)
	if err != nil {
		return -1, err
	}
	rank, err = r.getRank(ctx, event_id, user_id)
	if err != nil {
		return -1, err
	}

	ss, _ := r.Snapshots.LoadOrStore(event_id, NewWaitingQueueSnapshot())
	ss.Add(user_id, rank, score)

	return rank, nil
}

func (r *WaitingQueueRepository) Get(ctx context.Context, event_id string, user_id int64) (rank int64, err error) {
	ss, ok := r.Snapshots.Load(event_id)
	if !ok {
		return -1, ErrQueueEmpty //?
	}

	// Tier1: Redis에 직접 확인
	if ss.isTier1(ctx, user_id) {
		return r.getRank(ctx, event_id, user_id)
	}

	// Tier2: Snapshot에서 확인
	rank, ok = ss.GetRank(ctx, user_id)
	if !ok {
		return -1, ErrQueueEmpty //?
	}

	return rank, nil
}

func (r *WaitingQueueRepository) enqueue(ctx context.Context, event_id string, user_id int64) (score float64, err error) {
	score = float64(time.Now().UnixNano())

	err = r.client.ZAdd(ctx, buildKey(event_id), redis.Z{
		Score:  score,
		Member: user_id,
	}).Err()
	return score, err
}

var ErrQueueEmpty = errors.New("waiting room is empty")

func (r *WaitingQueueRepository) Pop(ctx context.Context, event_id string) (user_id int64, err error) {
	user_id, err = r.dequeue(ctx, event_id)
	if err != nil {
		return -1, err
	}
	if ss, ok := r.Snapshots.Load(event_id); ok {
		ss.Delete(user_id)
	}
	return user_id, nil
}

func (r *WaitingQueueRepository) dequeue(ctx context.Context, event_id string) (user_id int64, err error) {
	results, err := r.client.ZPopMin(ctx, buildKey(event_id), 1).Result() // minium score
	if err != nil {
		return -1, err
	}

	if len(results) == 0 {
		return -1, ErrQueueEmpty
	}

	return results[0].Member.(int64), nil
}

func (r *WaitingQueueRepository) getRank(ctx context.Context, event_id string, user_id int64) (rank int64, err error) {
	rank, err = r.client.ZRank(ctx, buildKey(event_id), strconv.FormatInt(user_id, 10)).Result()
	if err != nil {
		return -1, err
	}
	return rank, nil
}

// N분마다 반복?
func (r *WaitingQueueRepository) UpdateSnapshot(ctx context.Context, event_id string) error {
	// ZREVRANGE를 사용하여 전체 순위 리스트를 가져옴
	// 대용량일 경우 SCAN이나 분할 조회를 고려할 수 있으나
	// 수십만 건은 ZREVRANGE로 한 번에 가져와도 Redis 성능상 큰 무리가 없습니다.
	vals, err := r.client.ZRangeWithScores(ctx, buildKey(event_id), 0, -1).Result()
	if err != nil {
		return err
	}
	newData := make(map[int64]WaitingMember, len(vals))
	for i, z := range vals {
		newData[z.Member.(int64)] = WaitingMember{
			Rank:  int64(i + 1),
			Score: z.Score,
		}
	}

	// 원자적으로 스냅샷 교체
	ss, _ := r.Snapshots.LoadOrStore(event_id, NewWaitingQueueSnapshot())
	ss.Mu.Lock()
	ss.Members = newData //? 깊은복사 얕은복사
	ss.FetchedAt = time.Now()
	ss.Mu.Unlock()

	slog.Info("snapshot updated", "event_id", event_id, "size", len(newData))

	return nil
}

func buildKey(event_id string) string {
	return "waiting:" + event_id
}
