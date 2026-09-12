package dao

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/haierkeys/fast-note-sync-service/internal/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func newOnceInitTestDAO(t *testing.T) *Dao {
	t.Helper()
	queue := false
	cfg := config.DatabaseConfig{Type: "sqlite", Path: filepath.Join(t.TempDir(), "db.sqlite3"), EnableWriteQueue: &queue}
	db, err := NewEngine(cfg, zap.NewNop())
	require.NoError(t, err)
	d := New(db, context.Background(), WithConfig(&cfg), WithUserDatabaseConfig(&cfg), WithLogger(zap.NewNop()))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return d
}

// A freshly created user database is initialized lazily, on the first repository access.
// Requests that arrive while that first AutoMigrate is still running must wait for it:
// otherwise they query tables that are not created yet and fail with
// "SQL logic error: no such table" — which is what a brand new deployment hits, because a
// page load fires several of these requests in parallel.
// 新建的用户库在首次访问 Repository 时才延迟初始化。若在首次 AutoMigrate 尚未结束时到达的请求
// 不等待它完成，就会查询还不存在的表并报 "SQL logic error: no such table"——全新部署时正是如此，
// 因为一次页面加载会并行发出多个此类请求。
func TestQueryWithOnceInit_ConcurrentFirstUseInitializesTables(t *testing.T) {
	d := newOnceInitTestDAO(t)
	repo := NewBackupRepository(d)

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = repo.ListConfigs(context.Background(), 1)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "concurrent first use, goroutine %d", i)
	}
	// The history table is initialized by the same once-init and must exist as well.
	_, _, err := repo.ListHistory(context.Background(), 1, 1, 1, 10)
	require.NoError(t, err)
}

// Callers that arrive while the first initialization is still running must wait for it.
// This is the property that was missing: the "initialized" flag used to be published
// before the tables existed, so the late caller went straight to its query.
// 在首次初始化仍在进行时到达的调用必须等待其结束。这正是原先缺失的保证：过去"已初始化"标记在
// 建表完成之前就被写入，导致后到的调用直接去执行查询。
func TestQueryWithOnceInit_ConcurrentCallerWaitsForInitialization(t *testing.T) {
	d := newOnceInitTestDAO(t)

	initStarted := make(chan struct{})
	releaseInit := make(chan struct{})
	var initRuns int32

	go func() {
		d.QueryWithOnceInit(func(*gorm.DB) error {
			atomic.AddInt32(&initRuns, 1)
			close(initStarted)
			<-releaseInit
			return nil
		}, "gated#gated_key")
	}()

	<-initStarted

	secondCallerDone := make(chan struct{})
	go func() {
		defer close(secondCallerDone)
		d.QueryWithOnceInit(func(*gorm.DB) error {
			atomic.AddInt32(&initRuns, 1)
			return nil
		}, "gated#gated_key")
	}()

	select {
	case <-secondCallerDone:
		t.Fatal("second caller proceeded while the first initialization was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(releaseInit)

	select {
	case <-secondCallerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second caller did not unblock after the initialization completed")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&initRuns), "initialization must run exactly once")
}

// A failed initialization must not be cached: the next caller retries it instead of
// running against a database that never got its tables — the failure mode that made a
// transient error permanent for the rest of the process lifetime.
// 初始化失败不应被缓存：下一次调用会重试，而不是在一个永远没建成表的库上执行——那种失败模式会让
// 一次瞬时错误在进程剩余生命周期内变成永久错误。
func TestQueryWithOnceInit_RetriesAfterFailure(t *testing.T) {
	d := newOnceInitTestDAO(t)

	var attempts int
	boomed := errors.New("boom")
	failing := func(*gorm.DB) error {
		attempts++
		return boomed
	}

	d.QueryWithOnceInit(failing, "retry#retry_key")
	require.Equal(t, 1, attempts)
	d.QueryWithOnceInit(failing, "retry#retry_key")
	require.Equal(t, 2, attempts, "a failed initialization must be retried")

	var succeeding int
	ok := func(*gorm.DB) error {
		succeeding++
		return nil
	}
	d.QueryWithOnceInit(ok, "success#success_key")
	d.QueryWithOnceInit(ok, "success#success_key")
	require.Equal(t, 1, succeeding, "a successful initialization runs only once")
}
