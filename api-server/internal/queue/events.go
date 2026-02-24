package queue

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/gonflix/no24/api-server/internal/model"
)

func ReloadEvents(ctx context.Context) ([]model.EventDAO, error) {
	dsn := "root:admin123@tcp(mysql-service:3306)/ticketing?parseTime=true" // TIMESTAMP를 time.Time으로 변환
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		slog.Error("connect mysql db failed", "error", err)
		return nil, err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		slog.Error("mysql ping failed", "error", err)
		return nil, err
	}

	// 2. Select 쿼리 실행
	rows, err := db.Query("SELECT id, name, status, start_at, end_at, created_at, updated_at FROM events")
	if err != nil {
		slog.Error("query failed", "error", err)
		return nil, err
	}
	defer rows.Close()

	var events []model.EventDAO

	// 3. 결과 Row 스캔
	for rows.Next() {
		var e model.EventDAO
		err := rows.Scan(
			&e.ID,
			&e.Name,
			&e.Status,
			&e.StartAt,
			&e.EndAt,
			&e.CreatedAt,
			&e.UpdatedAt,
		)
		if err != nil {
			slog.Error("scan failed", "error", err)
			return nil, err
		}

		if e.Status == "N" {
			continue
		}
		events = append(events, e)
	}

	return events, nil
}
