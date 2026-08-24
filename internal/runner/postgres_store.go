package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore { return &PostgresStore{db: db} }

func (s *PostgresStore) Create(ctx context.Context, job Job) (Job, bool, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return Job{}, false, fmt.Errorf("encode payload: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, kind, payload, status, attempts, error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8)
		ON CONFLICT (id) DO NOTHING`,
		job.ID, job.Kind, payload, job.Status, job.Attempts, job.Error, job.CreatedAt, job.UpdatedAt)
	if err != nil {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, false, fmt.Errorf("read insert result: %w", err)
	}
	if rows == 1 {
		return job, true, nil
	}
	existing, ok, err := s.Get(ctx, job.ID)
	if err != nil {
		return Job{}, false, err
	}
	if !ok {
		return Job{}, false, errors.New("job conflict disappeared")
	}
	return existing, false, nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (Job, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, kind, payload, status, attempts, COALESCE(error, ''), created_at, updated_at
		FROM jobs WHERE id = $1`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("get job: %w", err)
	}
	return job, true, nil
}

func (s *PostgresStore) List(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, kind, payload, status, attempts, COALESCE(error, ''), created_at, updated_at
		FROM jobs ORDER BY created_at, id`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	out := []Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) Update(ctx context.Context, job Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET kind = $2, payload = $3, status = $4, attempts = $5,
			error = NULLIF($6, ''), updated_at = $7
		WHERE id = $1`,
		job.ID, job.Kind, payload, job.Status, job.Attempts, job.Error, job.UpdatedAt)
	if err != nil {
		return fmt.Errorf("update job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read update result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("update job: id %q not found", job.ID)
	}
	return nil
}

func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}
	return nil
}

func (s *PostgresStore) StartAttempt(ctx context.Context, jobID string, attempt int, startedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO job_attempts (job_id, attempt, started_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (job_id, attempt) DO UPDATE
		SET started_at = EXCLUDED.started_at, finished_at = NULL, outcome = NULL, error = NULL`,
		jobID, attempt, startedAt)
	if err != nil {
		return fmt.Errorf("start attempt: %w", err)
	}
	return nil
}

func (s *PostgresStore) FinishAttempt(ctx context.Context, jobID string, attempt int, outcome, message string, finishedAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE job_attempts
		SET finished_at = $3, outcome = $4, error = NULLIF($5, '')
		WHERE job_id = $1 AND attempt = $2`,
		jobID, attempt, finishedAt, outcome, message)
	if err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read attempt result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("finish attempt: job %q attempt %d not found", jobID, attempt)
	}
	return nil
}

type scanner interface {
	Scan(...any) error
}

func scanJob(row scanner) (Job, error) {
	var job Job
	var payload []byte
	if err := row.Scan(&job.ID, &job.Kind, &payload, &job.Status, &job.Attempts, &job.Error, &job.CreatedAt, &job.UpdatedAt); err != nil {
		return Job{}, err
	}
	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return Job{}, fmt.Errorf("decode payload: %w", err)
	}
	return job, nil
}

func OpenPostgres(ctx context.Context, databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return db, nil
}
