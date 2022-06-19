package queue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-test/deep"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"github.com/mergestat/pqueue/ddl"
	"github.com/mergestat/pqueue/introspect"
)

type Queue struct {
	pool *pgxpool.Pool
	// schema is the pg schema name to use for the queue table
	schema string
	// table is the pg table name to use for the queue table
	table string
}

// QueueOption is an option for configuring the queue
type QueueOption func(*Queue)

// NewQueue creates a new queue client for interacting with postgres as a queue
func NewQueue(pool *pgxpool.Pool, opts ...QueueOption) *Queue {
	q := &Queue{
		schema: "public",
		table:  "queue",
		pool:   pool,
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

// WithSchema sets the schema name to use for the queue table
func WithSchema(schema string) QueueOption {
	return func(q *Queue) {
		q.schema = schema
	}
}

// WithTable sets the table name to use for the queue table
func WithTable(table string) QueueOption {
	return func(q *Queue) {
		q.table = table
	}
}

// Ensure ensures that a table exists in PostgreSQL with the given Queue options (schema and table name).
// It creates the queue table if it doesn't yet exist, and reports an error if the existing table schema does not match what's needed.
func (q *Queue) Ensure(ctx context.Context) error {
	s := ddl.NewTableBuilder(q.table).
		SetSchema(q.schema).
		SetIfNotExists(true).
		AddColumn("id", "uuid", "PRIMARY KEY", "NOT NULL", "DEFAULT gen_random_uuid()").
		AddColumn("created_at", "timestamp with time zone", "DEFAULT now()", "NOT NULL").
		AddColumn("type", "text", "NOT NULL").
		AddColumn("data", "jsonb", "NOT NULL", "DEFAULT '{}'").
		AddColumn("priority", "int", "NOT NULL", "DEFAULT 1").
		AddColumn("status", "text", "NOT NULL", "DEFAULT 'QUEUED'").
		AddColumn("started_at", "timestamp with time zone").
		AddColumn("last_keep_alive", "timestamp with time zone").
		AddColumn("done_at", "timestamp with time zone").
		SQL()

	_, err := q.pool.Exec(ctx, s)
	if err != nil {
		return err
	}

	cols, err := introspect.LookupTableColumns(ctx, q.pool, q.schema, q.table)
	if err != nil {
		return err
	}

	// compare as a map, so that ordering doesn't matter
	colMap := make(map[string]*introspect.TableColumn)
	for _, col := range cols {
		colMap[col.Name] = col
	}

	cmp := deep.Equal(colMap, map[string]*introspect.TableColumn{
		"id": {
			Name:     "id",
			Type:     "uuid",
			Nullable: false,
			Default:  sql.NullString{String: "gen_random_uuid()", Valid: true},
		},
		"created_at": {
			Name:     "created_at",
			Type:     "timestamp with time zone",
			Nullable: false,
			Default:  sql.NullString{String: "now()", Valid: true},
		},
		"type": {
			Name:     "type",
			Type:     "text",
			Nullable: false,
			Default:  sql.NullString{},
		},
		"data": {
			Name:     "data",
			Type:     "jsonb",
			Nullable: false,
			Default:  sql.NullString{String: "'{}'::jsonb", Valid: true},
		},
		"priority": {
			Name:     "priority",
			Type:     "integer",
			Nullable: false,
			Default:  sql.NullString{String: "1", Valid: true},
		},
		"status": {
			Name:     "status",
			Type:     "text",
			Nullable: false,
			Default:  sql.NullString{String: "'QUEUED'::text", Valid: true},
		},
		"started_at": {
			Name:     "started_at",
			Type:     "timestamp with time zone",
			Nullable: true,
			Default:  sql.NullString{},
		},
		"last_keep_alive": {
			Name:     "last_keep_alive",
			Type:     "timestamp with time zone",
			Nullable: true,
			Default:  sql.NullString{},
		},
		"done_at": {
			Name:     "done_at",
			Type:     "timestamp with time zone",
			Nullable: true,
			Default:  sql.NullString{},
		},
	})

	if len(cmp) != 0 {
		return fmt.Errorf("unexpected schema: %s", cmp)
	}

	return nil
}

type EnqueueParams struct {
	Type     string
	Priority int
	Data     []byte
}

// Enqueue adds a new job to the queue
func (q *Queue) Enqueue(ctx context.Context, params *EnqueueParams) (string, error) {
	data := params.Data
	if data == nil {
		data = []byte("{}")
	}

	s := fmt.Sprintf(`INSERT INTO %s.%s (type, priority, data, status) VALUES ($1, $2, $3, $4) RETURNING id`, pq.QuoteIdentifier(q.schema), pq.QuoteIdentifier(q.table))

	row := q.pool.QueryRow(ctx, s, params.Type, params.Priority, data, "QUEUED")

	var id string
	if err := row.Scan(&id); err != nil {
		return "", err
	}

	return id, nil
}

// Job represents a job in the queue
type Job struct {
	ID            string
	CreatedAt     *time.Time
	Type          string
	Data          []byte
	Priority      int
	Status        string
	StartedAt     sql.NullTime
	LastKeepAlive sql.NullTime
	DoneAt        sql.NullTime
}

func (q *Queue) scanSingleJob(row pgx.Row) (*Job, error) {
	var id, jobType, status string
	var createdAt *time.Time
	var data []byte
	var priority int
	var startedAt, lastKeepAlive, doneAt sql.NullTime
	if err := row.Scan(&id, &createdAt, &jobType, &data, &priority, &status, &startedAt, &lastKeepAlive, &doneAt); err != nil {
		return nil, err
	}

	return &Job{
		ID:            id,
		CreatedAt:     createdAt,
		Type:          jobType,
		Data:          data,
		Priority:      priority,
		Status:        status,
		StartedAt:     startedAt,
		LastKeepAlive: lastKeepAlive,
		DoneAt:        doneAt,
	}, nil
}

func (q *Queue) Dequeue(ctx context.Context) (*Job, error) {
	s := fmt.Sprintf(`WITH dequeued AS (
		UPDATE %s.%s SET status = 'RUNNING'
		WHERE id IN (
			SELECT id FROM %s.%s
			WHERE status = 'QUEUED'
			ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
		) RETURNING id, created_at, type, data, priority, status, started_at, last_keep_alive, done_at
	)
	SELECT * FROM dequeued`,
		pq.QuoteIdentifier(q.schema),
		pq.QuoteIdentifier(q.table),
		pq.QuoteIdentifier(q.schema),
		pq.QuoteIdentifier(q.table),
	)

	row := q.pool.QueryRow(ctx, s)
	var j *Job
	var err error
	if j, err = q.scanSingleJob(row); err != nil {
		return nil, err
	}

	return j, nil
}

func (q *Queue) GetJobByID(ctx context.Context, jobID string) (*Job, error) {
	row := q.pool.QueryRow(ctx, fmt.Sprintf("SELECT * FROM %s.%s WHERE id = $1", pq.QuoteIdentifier(q.schema), pq.QuoteIdentifier(q.table)), jobID)
	var j *Job
	var err error
	if j, err = q.scanSingleJob(row); err != nil {
		return nil, err
	}
	return j, nil
}

func (q *Queue) SendJobKeepAlive(ctx context.Context, jobID string) error {
	_, err := q.pool.Exec(ctx, fmt.Sprintf("UPDATE %s.%s SET last_keep_alive = now() WHERE id = $1", pq.QuoteIdentifier(q.schema), pq.QuoteIdentifier(q.table)), jobID)
	return err
}

func (q *Queue) MarkJobDone(ctx context.Context, jobID string) error {
	_, err := q.pool.Exec(ctx, fmt.Sprintf("UPDATE %s.%s SET status = 'DONE' WHERE id = $1", pq.QuoteIdentifier(q.schema), pq.QuoteIdentifier(q.table)), jobID)
	return err
}

func (q *Queue) GetSchemaName() string {
	return q.schema
}

func (q *Queue) GetTableName() string {
	return q.table
}
