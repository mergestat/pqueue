package queue_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mergestat/pgq/queue"
)

var (
	postgresConn = os.Getenv("POSTGRES_CONNECTION")
)

func TestEnsureOK(t *testing.T) {
	if postgresConn == "" {
		t.SkipNow()
	}

	ctx := context.TODO()

	var p *pgxpool.Pool
	var err error
	if p, err = pgxpool.Connect(ctx, postgresConn); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	q := queue.NewQueue(p)
	if err = q.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueOK(t *testing.T) {
	if postgresConn == "" {
		t.SkipNow()
	}

	ctx := context.TODO()

	var p *pgxpool.Pool
	var err error
	if p, err = pgxpool.Connect(ctx, postgresConn); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	q := queue.NewQueue(p)

	var id string
	if id, err = q.Enqueue(ctx, &queue.EnqueueParams{Type: "testing", Priority: 1, Data: []byte(`{"hello": "world"}`)}); err != nil {
		t.Fatal(err)
	}

	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if job.Type != "testing" {
		t.Fatalf("unexpected job type: %s", job.Type)
	}

	if job.Status != "RUNNING" {
		t.Fatalf("expected job to be running, got status: %s", job.Status)
	}

	if job.ID != id {
		t.Fatalf("unexpected job ID: %s", job.ID)
	}

	if job.LastKeepAlive.Valid {
		t.Fatalf("expected last_keep_alive to be nil, got: %s", job.LastKeepAlive.Time.String())
	}

	if err := q.SendJobKeepAlive(ctx, job.ID); err != nil {
		t.Fatal(err)
	}

	if job, err = q.GetJobByID(ctx, job.ID); err != nil {
		t.Fatal(err)
	}

	if !job.LastKeepAlive.Valid {
		t.Fatal("expected last_keep_alive to be set")
	}
}

func TestPriorityDequeueOK(t *testing.T) {
	if postgresConn == "" {
		t.SkipNow()
	}

	ctx := context.TODO()

	var p *pgxpool.Pool
	var err error
	if p, err = pgxpool.Connect(ctx, postgresConn); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	q := queue.NewQueue(p)

	if _, err = q.Enqueue(ctx, &queue.EnqueueParams{Type: "priority-test", Priority: 1}); err != nil {
		t.Fatal(err)
	}

	var higherPriorityID string
	if higherPriorityID, err = q.Enqueue(ctx, &queue.EnqueueParams{Type: "priority-test", Priority: 10}); err != nil {
		t.Fatal(err)
	}

	job, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if job.ID != higherPriorityID {
		t.Fatalf("expected to dequeue job: %s, got: %s", higherPriorityID, job.ID)
	}
}
