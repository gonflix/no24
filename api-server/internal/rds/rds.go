package rds

// type RankInfo struct {
// 	Rank  int64   `json:"rank"`
// 	Score float64 `json:"score"`
// }
// type Snapshot struct {
// 	LastUpdate time.Time
// 	Data       map[int64]RankInfo
// 	Mu         *xsync.RBMutex
// }

// // Redis의 Sorted Set을 활용한 Priority Queue 패턴
// type rdsClient struct {
// 	client   *redis.Client
// 	snapshot *Snapshot
// }

// var rdsClientInstance *rdsClient

// func InitRdsClient() *rdsClient {
// 	rdsClientInstance = &rdsClient{
// 		snapshot: &Snapshot{
// 			Data: make(map[int64]RankInfo),
// 			Mu:   xsync.NewRBMutex(),
// 		},
// 		client: redis.NewClient(&redis.Options{
// 			Addr:     "localhost:6379",
// 			Password: "", // no password set
// 			DB:       0,  // use default DB
// 		}),
// 	}
// 	return rdsClientInstance
// }
// func (r *rdsClient) Close() { r.client.Close() }

// func buildKey(event_id string) string { return "waiting:" + event_id }

// func (r *rdsClient) addUser(ctx context.Context, user_id int64, event_id string) error {
// 	key := buildKey(event_id)
// 	return r.client.ZAdd(ctx, key, redis.Z{
// 		Score:  float64(time.Now().UnixNano()),
// 		Member: user_id,
// 	}).Err()
// }

// func getUser(ctx context.Context, user_id int64, event_id string) (int64, error) {
// 	key := buildKey(event_id)
// 	rank, err := rdsClientInstance.client.ZRank(ctx, key, strconv.FormatInt(user_id, 10)).Result()
// 	if err != nil {
// 		return -1, err
// 	}
// 	return rank, nil
// }

// var ErrWaitingRoomEmpty = errors.New("waiting room is empty")

// func PopFromWaitingRoom(ctx context.Context, event_id string) (user_id int64, err error) {
// 	key := buildKey(event_id)

// 	results, err := rdsClientInstance.client.ZPopMin(ctx, key, 1).Result() // minium score
// 	if err != nil {
// 		return -1, err
// 	}

// 	if len(results) == 0 {
// 		return -1, ErrWaitingRoomEmpty
// 	}

// 	return results[0].Member.(int64), nil
// }

// func UpdateSnapshot(ctx context.Context, event_id string) {
// 	key := buildKey(event_id)

// 	ticker := time.NewTicker(10 * time.Second) // 하위 99%용 갱신 주기

// 	for range ticker.C {
// 		// ZREVRANGE를 사용하여 전체 순위 리스트를 가져옴
// 		// 대용량일 경우 SCAN이나 분할 조회를 고려할 수 있으나
// 		// 수십만 건은 ZREVRANGE로 한 번에 가져와도 Redis 성능상 큰 무리가 없습니다.
// 		users, err := rdsClientInstance.client.ZRevRangeWithScores(ctx, key, 0, -1).Result()
// 		if err != nil {
// 			log.Printf("Snapshot error: %v", err)
// 			continue
// 		}

// 		newData := make(map[int64]RankInfo, len(users))
// 		for i, u := range users {
// 			newData[u.Member.(int64)] = RankInfo{
// 				Rank:  int64(i + 1),
// 				Score: u.Score,
// 			}
// 		}

// 		// 원자적으로 스냅샷 교체
// 		redisClient.snapshot.Mu.Lock()
// 		redisClient.snapshot.Data = newData //? 깊은복사 얕은복사
// 		redisClient.snapshot.LastUpdate = time.Now()
// 		redisClient.snapshot.Mu.Unlock()
// 	}
// }

// func GetUserFromSnapshot(ctx context.Context, user_id int64) (RankInfo, int, bool) {
// 	rtkn := redisClient.snapshot.Mu.RLock()
// 	defer redisClient.snapshot.Mu.RUnlock(rtkn)

// 	info, exists := redisClient.snapshot.Data[user_id]
// 	return info, len(redisClient.snapshot.Data), exists
// }

// func GetSnapshotSize() int64 {
// 	rtkn := redisClient.snapshot.Mu.RLock()
// 	defer redisClient.snapshot.Mu.RUnlock(rtkn)

// 	return int64(len(redisClient.snapshot.Data))
// }

// func PopUserFromSnapshot(user_id int64) {
// 	redisClient.snapshot.Mu.Lock()
// 	defer redisClient.snapshot.Mu.Unlock()

// 	delete(redisClient.snapshot.Data, user_id)
// }

// func IsSnapshotOutdated(event_id string) bool {
// 	return redisClient.snapshot.LastUpdate.Add(3 * time.Minute).Before(time.Now())
// }
