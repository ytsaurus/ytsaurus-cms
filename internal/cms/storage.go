package cms

import (
	"context"
	"fmt"
	"time"

	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

//go:generate mockgen -destination=./mocks/storage_mock.go -package mocks . Storage

type Storage interface {
	// Add stores new task.
	Add(ctx context.Context, task *models.Task) error
	// Update overwrites existing task.
	Update(ctx context.Context, task *models.Task) error
	// GetAll returns all CMS tasks.
	GetAll(ctx context.Context) ([]*models.Task, error)
	// Get returns task with given id.
	Get(ctx context.Context, id walle.TaskID) (*models.Task, error)
	// GetHostTasks returns all active tasks with provided host.
	GetHostTasks(ctx context.Context, host string) ([]*models.Task, error)
	// Confirm marks task with given id as confirmed by requester.
	Confirm(ctx context.Context, id walle.TaskID, requester string) (*models.Task, error)
	// Delete removes task with given id.
	Delete(ctx context.Context, id walle.TaskID) error
}

var TaskNotFoundErr = xerrors.New("task not found")

type tasksTableKey struct {
	ID walle.TaskID `yson:"id,key"`
}

// Storage implementation that uses YT dynamic tables.
type dynamicTableStorage struct {
	yc    yt.Client
	table ypath.Path
}

// NewStorage creates storage that uses YT dynamic tables.
//
// Is not responsible for table mounting.
func NewStorage(yc yt.Client, table ypath.Path) Storage {
	return &dynamicTableStorage{
		yc:    yc,
		table: table,
	}
}

// Add inserts CMS task into dynamic table.
func (s *dynamicTableStorage) Add(ctx context.Context, task *models.Task) error {
	now := yson.Time(time.Now().UTC())
	task.CreatedAt = now
	task.UpdatedAt = now

	return s.update(ctx, task, false)
}

// Update overwrites existing cms task.
//
// DeletionRequested* and ConfirmationRequested* fields will not be overwritten.
func (s *dynamicTableStorage) Update(ctx context.Context, task *models.Task) error {
	task.UpdatedAt = yson.Time(time.Now().UTC())

	return s.update(ctx, task, true)
}

func (s *dynamicTableStorage) update(ctx context.Context, task *models.Task, overwrite bool) error {
	tx, err := s.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	query := fmt.Sprintf("* from [%s] where id='%s'", s.table, task.ID)
	r, err := tx.SelectRows(ctx, query, nil)
	if err != nil {
		return err
	}

	tasks, err := scanTasks(r)
	if err != nil {
		return err
	}

	if len(tasks) > 1 {
		panic(xerrors.Errorf("task id must be unique; got %d tasks: %+v", len(tasks), tasks))
	}

	if len(tasks) == 1 && overwrite {
		task.ConfirmationRequested = tasks[0].ConfirmationRequested
		task.ConfirmationRequestedAt = tasks[0].ConfirmationRequestedAt
		task.ConfirmationRequestedBy = tasks[0].ConfirmationRequestedBy
		task.DeletionRequested = tasks[0].DeletionRequested
		task.DeletionRequestedAt = tasks[0].DeletionRequestedAt
	} else if len(tasks) == 1 && !overwrite {
		panic(xerrors.Errorf("task id must be unique; got %d tasks: %+v", len(tasks), tasks))
	}

	rows := []interface{}{task}
	if err := tx.InsertRows(ctx, s.table, rows, nil); err != nil {
		return xerrors.Errorf("dyn-table insert failed: %w", err)
	}

	return tx.Commit()
}

// GetAll selects all CMS tasks from dynamic table.
func (s *dynamicTableStorage) GetAll(ctx context.Context) ([]*models.Task, error) {
	query := fmt.Sprintf("* from [%s] where processing_state != '%s'", s.table, models.StateFinished)
	return s.selectTasks(ctx, query)
}

// Get selects task with given id.
//
// Returns error wrapping TaskNotFoundErr when task was not found.
func (s *dynamicTableStorage) Get(ctx context.Context, id walle.TaskID) (*models.Task, error) {
	query := fmt.Sprintf("* from [%s] where id='%s'", s.table, id)
	tasks, err := s.selectTasks(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(tasks) > 1 {
		panic(xerrors.Errorf("task id must be unique; got %d tasks: %+v", len(tasks), tasks))
	}
	if len(tasks) == 0 {
		return nil, xerrors.Errorf("unable to find task id %s: %w", id, TaskNotFoundErr)
	}
	return tasks[0], nil
}

// GetHostTasks selects all active tasks with given host.
func (s *dynamicTableStorage) GetHostTasks(ctx context.Context, host string) ([]*models.Task, error) {
	query := fmt.Sprintf(
		"* from [%s] where processing_state != '%s' and not confirmation_requested and list_contains(hosts, '%s')",
		s.table, models.StateFinished, host)
	return s.selectTasks(ctx, query)
}

// Confirm marks task with given id as confirmed.
//
// Returns task with modified confirmation request data.
func (s *dynamicTableStorage) Confirm(ctx context.Context, id walle.TaskID, requester string) (*models.Task, error) {
	tx, err := s.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Abort() }()

	task, err := s.loadTask(ctx, id, tx)
	if err != nil {
		return nil, err
	}

	now := yson.Time(time.Now().UTC())
	task.UpdatedAt = now

	task.ConfirmationRequested = true
	task.ConfirmationRequestedAt = now
	task.ConfirmationRequestedBy = requester

	rows := []interface{}{task}
	if err := tx.InsertRows(ctx, s.table, rows, nil); err != nil {
		return nil, xerrors.Errorf("dyn-table insert failed: %w", err)
	}

	return task, tx.Commit()
}

// Delete marks task with given id as deleted.
func (s *dynamicTableStorage) Delete(ctx context.Context, id walle.TaskID) error {
	tx, err := s.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	task, err := s.loadTask(ctx, id, tx)
	if err != nil {
		return err
	}

	now := yson.Time(time.Now().UTC())
	task.UpdatedAt = now

	task.DeletionRequestedAt = now
	task.DeletionRequested = true

	rows := []interface{}{task}
	if err := tx.InsertRows(ctx, s.table, rows, nil); err != nil {
		return xerrors.Errorf("dyn-table insert failed: %w", err)
	}

	return tx.Commit()
}

// selectTasks executes query and deserializes result into task slice.
func (s *dynamicTableStorage) selectTasks(ctx context.Context, query string) ([]*models.Task, error) {
	r, err := s.yc.SelectRows(ctx, query, nil)
	if err != nil {
		return nil, xerrors.Errorf("dyn-table select failed: %w", err)
	}
	defer func() { _ = r.Close() }()

	return scanTasks(r)
}

// loadTask loads task with given id within transaction.
//
// Returns error wrapping TaskNotFoundErr if task was not found.
func (s *dynamicTableStorage) loadTask(ctx context.Context, id walle.TaskID, tx yt.TabletTx) (*models.Task, error) {
	key := &tasksTableKey{ID: id}
	r, err := tx.LookupRows(ctx, s.table, []interface{}{key}, nil)
	if err != nil {
		return nil, err
	}

	tasks, err := scanTasks(r)
	if err != nil {
		return nil, err
	}

	if len(tasks) > 2 {
		panic(xerrors.Errorf("task id must be unique; got %d tasks: %+v", len(tasks), tasks))
	}

	if len(tasks) == 0 {
		return nil, xerrors.Errorf("unable to find task (%s): %w", id, TaskNotFoundErr)
	}

	return tasks[0], nil
}

// scanTasks serializes tasks into slice.
func scanTasks(r yt.TableReader) ([]*models.Task, error) {
	var tasks []*models.Task
	for r.Next() {
		var t models.Task
		if err := r.Scan(&t); err != nil {
			return nil, err
		}
		tasks = append(tasks, &t)
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	return tasks, nil
}
