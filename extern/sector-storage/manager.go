package sectorstorage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"github.com/filecoin-project/lotus/extern/sector-storage/fsutil"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

var log = logging.Logger("advmgr")

var ErrNoWorkers = errors.New("no suitable workers found")

type URLs []string

type Worker interface {
	storiface.WorkerCalls

	TaskTypes(context.Context) (map[sealtasks.TaskType]struct{}, error)

	// Returns paths accessible to the worker
	Paths(context.Context) ([]stores.StoragePath, error)

	Info(context.Context) (storiface.WorkerInfo, error)

	Session(context.Context) (uuid.UUID, error)

	Close() error // TODO: do we need this?
}

type SectorManager interface {
	ReadPiece(context.Context, io.Writer, storage.SectorRef, storiface.UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) error

	ffiwrapper.StorageSealer
	storage.Prover
	storiface.WorkerReturn
	FaultTracker
}

type WorkerID uuid.UUID // worker session UUID
var ClosedWorkerID = uuid.UUID{}

func (w WorkerID) String() string {
	return uuid.UUID(w).String()
}

type Manager struct {
	ls         stores.LocalStorage  // miner本地存储: miner自身存储
	storage    *stores.Remote       // 封装本地真实存储
	localStore *stores.Local        // 本地真实存储
	remoteHnd  *stores.FetchHandler // 提供API接口：获取本地存储的信息.
	index      stores.SectorIndex

	sched *scheduler // 管理worker

	storage.Prover // 实现winning post和window post

	workLk sync.Mutex
	work   *statestore.StateStore // 保存查询任务的存储

	callToWork map[storiface.CallID]WorkID
	// used when we get an early return and there's no callToWork mapping
	callRes map[storiface.CallID]chan result

	results map[WorkID]result
	waitRes map[WorkID]chan struct{}
}

type result struct {
	r   interface{}
	err error
}

type SealerConfig struct {
	ParallelFetchLimit int

	// Local worker config
	AllowAddPiece   bool
	AllowPreCommit1 bool
	AllowPreCommit2 bool
	AllowCommit     bool
	AllowUnseal     bool
}

type StorageAuth http.Header

type WorkerStateStore *statestore.StateStore
type ManagerStateStore *statestore.StateStore

// miner创建一个新的Manager
/*
ls: miner的本地存储（不是用于存储封装临时数据，或者扇区的永久存储）。
保存元数据和配置文件。

si: 管理扇区封装临时存储，扇区永久存储。

urls: miner自身的API：http://xxxx:2345/remote
*/
func New(ctx context.Context, ls stores.LocalStorage, si stores.SectorIndex, sc SealerConfig, urls URLs, sa StorageAuth, wss WorkerStateStore, mss ManagerStateStore) (*Manager, error) {
	// 创建本地存储：miner自身存储、封装临时存储、扇区永久存储.
	lstor, err := stores.NewLocal(ctx, ls, si, urls)
	if err != nil {
		return nil, err
	}

	// window post和winning post
	prover, err := ffiwrapper.New(&readonlyProvider{stor: lstor, index: si})
	if err != nil {
		return nil, xerrors.Errorf("creating prover instance: %w", err)
	}

	// 将本地存储，包装为一个远程存储.
	stor := stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit)

	m := &Manager{
		ls:         ls,
		storage:    stor,
		localStore: lstor,
		remoteHnd:  &stores.FetchHandler{Local: lstor},
		index:      si,

		sched: newScheduler(),

		Prover: prover,

		// 这里保存了任务调度给worker的信息：
		/*
			ID WorkID	// 工作ID
			Status WorkStatus	// 工作状态：started/running/done
			WorkerCall storiface.CallID // Set when entering wsRunning
			WorkError  string           // Status = wsDone, set when failed to start work
			WorkerHostname string // 最后处理这个任务的worker名字
			StartTime      int64  // 任务开始时间：unix时间戳
		*/
		// 初始化时从磁盘加载之前的数据。
		work: mss,

		callToWork: map[storiface.CallID]WorkID{},
		callRes:    map[storiface.CallID]chan result{},
		results:    map[WorkID]result{},
		waitRes:    map[WorkID]chan struct{}{},
	}

	/*
		从mss(manage state store)中恢复work状态
	*/
	m.setupWorkTracker()

	// 启动调度器
	go m.sched.runSched()

	// 根据miner启动的参数，收集本地worker支持的任务类型。
	localTasks := []sealtasks.TaskType{
		sealtasks.TTCommit1, sealtasks.TTFinalize, sealtasks.TTFetch, sealtasks.TTReadUnsealed,
	}
	if sc.AllowAddPiece {
		localTasks = append(localTasks, sealtasks.TTAddPiece)
	}
	if sc.AllowPreCommit1 {
		localTasks = append(localTasks, sealtasks.TTPreCommit1)
	}
	if sc.AllowPreCommit2 {
		localTasks = append(localTasks, sealtasks.TTPreCommit2)
	}
	if sc.AllowCommit {
		localTasks = append(localTasks, sealtasks.TTCommit2)
	}
	if sc.AllowUnseal {
		localTasks = append(localTasks, sealtasks.TTUnseal)
	}

	// miner一启动，增加本地worker
	err = m.AddWorker(ctx, NewLocalWorker(WorkerConfig{
		TaskTypes: localTasks,
	}, stor, lstor, si, m, wss))
	if err != nil {
		return nil, xerrors.Errorf("adding local worker: %w", err)
	}

	return m, nil
}

func (m *Manager) AddLocalStorage(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return xerrors.Errorf("expanding local path: %w", err)
	}

	if err := m.localStore.OpenPath(ctx, path); err != nil {
		return xerrors.Errorf("opening local path: %w", err)
	}

	if err := m.ls.SetStorage(func(sc *stores.StorageConfig) {
		sc.StoragePaths = append(sc.StoragePaths, stores.LocalPath{Path: path})
	}); err != nil {
		return xerrors.Errorf("get storage config: %w", err)
	}
	return nil
}

// 让调度器运行worker
func (m *Manager) AddWorker(ctx context.Context, w Worker) error {
	return m.sched.runWorker(ctx, w)
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.remoteHnd.ServeHTTP(w, r)
}

func schedNop(context.Context, Worker) error {
	return nil
}

func (m *Manager) schedFetch(sector storage.SectorRef, ft storiface.SectorFileType, ptype storiface.PathType, am storiface.AcquireMode) func(context.Context, Worker) error {
	return func(ctx context.Context, worker Worker) error {
		_, err := m.waitSimpleCall(ctx)(worker.Fetch(ctx, sector, ft, ptype, am))
		return err
	}
}

func (m *Manager) readPiece(sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, rok *bool) func(ctx context.Context, w Worker) error {
	return func(ctx context.Context, w Worker) error {
		r, err := m.waitSimpleCall(ctx)(w.ReadPiece(ctx, sink, sector, offset, size))
		if err != nil {
			return err
		}
		if r != nil {
			*rok = r.(bool)
		}
		return nil
	}
}

func (m *Manager) tryReadUnsealedPiece(ctx context.Context, sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (foundUnsealed bool, readOk bool, selector WorkerSelector, returnErr error) {

	// acquire a lock purely for reading unsealed sectors
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTNone); err != nil {
		returnErr = xerrors.Errorf("acquiring read sector lock: %w", err)
		return
	}

	// passing 0 spt because we only need it when allowFetch is true
	best, err := m.index.StorageFindSector(ctx, sector.ID, storiface.FTUnsealed, 0, false)
	if err != nil {
		returnErr = xerrors.Errorf("read piece: checking for already existing unsealed sector: %w", err)
		return
	}

	foundUnsealed = len(best) > 0
	if foundUnsealed { // append to existing
		// There is unsealed sector, see if we can read from it

		selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)

		err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove),
			m.readPiece(sink, sector, offset, size, &readOk))
		if err != nil {
			returnErr = xerrors.Errorf("reading piece from sealed sector: %w", err)
		}
	} else {
		selector = newAllocSelector(m.index, storiface.FTUnsealed, storiface.PathSealing)
	}
	return
}

func (m *Manager) ReadPiece(ctx context.Context, sink io.Writer, sector storage.SectorRef, offset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize, ticket abi.SealRandomness, unsealed cid.Cid) error {
	foundUnsealed, readOk, selector, err := m.tryReadUnsealedPiece(ctx, sink, sector, offset, size)
	if err != nil {
		return err
	}
	if readOk {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTUnsealed); err != nil {
		return xerrors.Errorf("acquiring unseal sector lock: %w", err)
	}

	unsealFetch := func(ctx context.Context, worker Worker) error {
		if _, err := m.waitSimpleCall(ctx)(worker.Fetch(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.PathSealing, storiface.AcquireCopy)); err != nil {
			return xerrors.Errorf("copy sealed/cache sector data: %w", err)
		}

		if foundUnsealed {
			if _, err := m.waitSimpleCall(ctx)(worker.Fetch(ctx, sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove)); err != nil {
				return xerrors.Errorf("copy unsealed sector data: %w", err)
			}
		}
		return nil
	}

	if unsealed == cid.Undef {
		return xerrors.Errorf("cannot unseal piece (sector: %d, offset: %d size: %d) - unsealed cid is undefined", sector, offset, size)
	}
	err = m.sched.Schedule(ctx, sector, sealtasks.TTUnseal, selector, unsealFetch, func(ctx context.Context, w Worker) error {
		// TODO: make restartable
		_, err := m.waitSimpleCall(ctx)(w.UnsealPiece(ctx, sector, offset, size, ticket, unsealed))
		return err
	})
	if err != nil {
		return err
	}

	selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTReadUnsealed, selector, m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove),
		m.readPiece(sink, sector, offset, size, &readOk))
	if err != nil {
		return xerrors.Errorf("reading piece from sealed sector: %w", err)
	}

	if !readOk {
		return xerrors.Errorf("failed to read unsealed piece")
	}

	return nil
}

func (m *Manager) NewSector(ctx context.Context, sector storage.SectorRef) error {
	log.Warnf("stub NewSector")
	return nil
}

func (m *Manager) AddPiece(ctx context.Context, sector storage.SectorRef, existingPieces []abi.UnpaddedPieceSize, sz abi.UnpaddedPieceSize, r io.Reader) (abi.PieceInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTUnsealed); err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var selector WorkerSelector
	var err error
	if len(existingPieces) == 0 { // new
		selector = newAllocSelector(m.index, storiface.FTUnsealed, storiface.PathSealing)
	} else { // use existing
		selector = newExistingSelector(m.index, sector.ID, storiface.FTUnsealed, false)
	}

	var out abi.PieceInfo
	err = m.sched.Schedule(ctx, sector, sealtasks.TTAddPiece, selector, schedNop, func(ctx context.Context, w Worker) error {
		callID, err := w.AddPiece(ctx, sector, existingPieces, sz, r)
		log.Debugf("^^^^^^^^ AddPiece return callID: [%v]\n", callID)
		waitFunc := m.waitSimpleCall(ctx)
		p, err := waitFunc(callID, err)
		if err != nil {
			return err
		}
		if p != nil {
			out = p.(abi.PieceInfo)
		}
		return nil
	})

	log.Debugf("^^^^^^^^ out.PieceCID: [%v]\n", out.PieceCID)

	return out, err
}

func (m *Manager) SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (out storage.PreCommit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 将（扇区信息、扇区任务类型、tick、pieces）组合成任务（work）信息，
	// 在manager的datastore中查询，是否存在这个任务，如果不存在则manager保存这个任务的信息，
	// 并返回任务id，和cancel函数。
	// 如果存在则直接返回workID.
	// wait标志，指明了这个任务，是否已经在运行。
	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTPreCommit1, sector, ticket, pieces)
	if err != nil {
		return nil, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	// 等待旧任务（work）完成的函数.
	// 也就是说，miner重启过，但是这个任务依然被某个worker运行.
	// 作为miner自然要等待旧的任务完成，而不是重新开始。
	waitRes := func() {
		//
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.PreCommit1Out)
		}
	}

	// 如果任务已经在运行（running）就等待任务完成，并返回之前同样任务的执行结果。
	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	// manager（矿工）为扇区的读、写动作执行锁定，如果无法锁定会一直等待之前的锁释放。
	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTUnsealed, storiface.FTSealed|storiface.FTCache); err != nil {
		return nil, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// TODO: also consider where the unsealed data sits

	selector := newAllocSelector(m.index, storiface.FTCache|storiface.FTSealed, storiface.PathSealing)

	// 让调度器开始开始调度扇区任务
	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit1, selector,
		// 执行扇区任务之前，调度器需要预先执行的动作。
		// 这里的动作是，调用worker的Fetch接口，让worker提前把扇区复制到本地.
		m.schedFetch(sector, storiface.FTUnsealed, storiface.PathSealing, storiface.AcquireMove),
		// 这个函数就是找到worker后，真正要做的动作。
		// 这里就是真正的调用worker的SealPreCommit1接口，让worker执行P1.
		func(ctx context.Context, w Worker) error {
			// 调用worker的接口执行P1，然会一个调用ID.
			callID, err := w.SealPreCommit1(ctx, sector, ticket, pieces)
			waitFunc := m.startWork(ctx, w, wk)
			// 等待本地调用完成。
			err = waitFunc(callID, err)
			if err != nil {
				return err
			}

			waitRes()
			return nil
		})
	if err != nil {
		return nil, err
	}

	return out, waitErr
}

func (m *Manager) SealPreCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.PreCommit1Out) (out storage.SectorCids, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTPreCommit2, sector, phase1Out)
	if err != nil {
		return storage.SectorCids{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.SectorCids)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.SectorCids{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, true)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTPreCommit2, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		err := m.startWork(ctx, w, wk)(w.SealPreCommit2(ctx, sector, phase1Out))
		if err != nil {
			return err
		}

		waitRes()
		return nil
	})
	if err != nil {
		return storage.SectorCids{}, err
	}

	return out, waitErr
}

func (m *Manager) SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (out storage.Commit1Out, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTCommit1, sector, ticket, seed, pieces, cids)
	if err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.Commit1Out)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTSealed, storiface.FTCache); err != nil {
		return storage.Commit1Out{}, xerrors.Errorf("acquiring sector lock: %w", err)
	}

	// NOTE: We set allowFetch to false in so that we always execute on a worker
	// with direct access to the data. We want to do that because this step is
	// generally very cheap / fast, and transferring data is not worth the effort
	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit1, selector, m.schedFetch(sector, storiface.FTCache|storiface.FTSealed, storiface.PathSealing, storiface.AcquireMove), func(ctx context.Context, w Worker) error {
		err := m.startWork(ctx, w, wk)(w.SealCommit1(ctx, sector, ticket, seed, pieces, cids))
		if err != nil {
			return err
		}

		waitRes()
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, waitErr
}

func (m *Manager) SealCommit2(ctx context.Context, sector storage.SectorRef, phase1Out storage.Commit1Out) (out storage.Proof, err error) {
	wk, wait, cancel, err := m.getWork(ctx, sealtasks.TTCommit2, sector, phase1Out)
	if err != nil {
		return storage.Proof{}, xerrors.Errorf("getWork: %w", err)
	}
	defer cancel()

	var waitErr error
	waitRes := func() {
		p, werr := m.waitWork(ctx, wk)
		if werr != nil {
			waitErr = werr
			return
		}
		if p != nil {
			out = p.(storage.Proof)
		}
	}

	if wait { // already in progress
		waitRes()
		return out, waitErr
	}

	selector := newTaskSelector()

	err = m.sched.Schedule(ctx, sector, sealtasks.TTCommit2, selector, schedNop, func(ctx context.Context, w Worker) error {
		err := m.startWork(ctx, w, wk)(w.SealCommit2(ctx, sector, phase1Out))
		if err != nil {
			return err
		}

		waitRes()
		return nil
	})

	if err != nil {
		return nil, err
	}

	return out, waitErr
}

func (m *Manager) FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTSealed|storiface.FTUnsealed|storiface.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	unsealed := storiface.FTUnsealed
	{
		unsealedStores, err := m.index.StorageFindSector(ctx, sector.ID, storiface.FTUnsealed, 0, false)
		if err != nil {
			return xerrors.Errorf("finding unsealed sector: %w", err)
		}

		if len(unsealedStores) == 0 { // Is some edge-cases unsealed sector may not exist already, that's fine
			unsealed = storiface.FTNone
		}
	}

	selector := newExistingSelector(m.index, sector.ID, storiface.FTCache|storiface.FTSealed, false)

	err := m.sched.Schedule(ctx, sector, sealtasks.TTFinalize, selector,
		m.schedFetch(sector, storiface.FTCache|storiface.FTSealed|unsealed, storiface.PathSealing, storiface.AcquireMove),
		func(ctx context.Context, w Worker) error {
			_, err := m.waitSimpleCall(ctx)(w.FinalizeSector(ctx, sector, keepUnsealed))
			return err
		})
	if err != nil {
		return err
	}

	fetchSel := newAllocSelector(m.index, storiface.FTCache|storiface.FTSealed, storiface.PathStorage)
	moveUnsealed := unsealed
	{
		if len(keepUnsealed) == 0 {
			moveUnsealed = storiface.FTNone
		}
	}

	err = m.sched.Schedule(ctx, sector, sealtasks.TTFetch, fetchSel,
		m.schedFetch(sector, storiface.FTCache|storiface.FTSealed|moveUnsealed, storiface.PathStorage, storiface.AcquireMove),
		func(ctx context.Context, w Worker) error {
			_, err := m.waitSimpleCall(ctx)(w.MoveStorage(ctx, sector, storiface.FTCache|storiface.FTSealed|moveUnsealed))
			return err
		})
	if err != nil {
		return xerrors.Errorf("moving sector to storage: %w", err)
	}

	return nil
}

func (m *Manager) ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) error {
	log.Warnw("ReleaseUnsealed todo")
	return nil
}

func (m *Manager) Remove(ctx context.Context, sector storage.SectorRef) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.index.StorageLock(ctx, sector.ID, storiface.FTNone, storiface.FTSealed|storiface.FTUnsealed|storiface.FTCache); err != nil {
		return xerrors.Errorf("acquiring sector lock: %w", err)
	}

	var err error

	if rerr := m.storage.Remove(ctx, sector.ID, storiface.FTSealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (sealed): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector.ID, storiface.FTCache, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (cache): %w", rerr))
	}
	if rerr := m.storage.Remove(ctx, sector.ID, storiface.FTUnsealed, true); rerr != nil {
		err = multierror.Append(err, xerrors.Errorf("removing sector (unsealed): %w", rerr))
	}

	return err
}

func (m *Manager) ReturnAddPiece(ctx context.Context, callID storiface.CallID, pi abi.PieceInfo, err *storiface.CallError) error {
	return m.returnResult(callID, pi, err)
}

func (m *Manager) ReturnSealPreCommit1(ctx context.Context, callID storiface.CallID, p1o storage.PreCommit1Out, err *storiface.CallError) error {
	return m.returnResult(callID, p1o, err)
}

func (m *Manager) ReturnSealPreCommit2(ctx context.Context, callID storiface.CallID, sealed storage.SectorCids, err *storiface.CallError) error {
	return m.returnResult(callID, sealed, err)
}

func (m *Manager) ReturnSealCommit1(ctx context.Context, callID storiface.CallID, out storage.Commit1Out, err *storiface.CallError) error {
	return m.returnResult(callID, out, err)
}

func (m *Manager) ReturnSealCommit2(ctx context.Context, callID storiface.CallID, proof storage.Proof, err *storiface.CallError) error {
	return m.returnResult(callID, proof, err)
}

func (m *Manager) ReturnFinalizeSector(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnReleaseUnsealed(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnMoveStorage(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnUnsealPiece(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) ReturnReadPiece(ctx context.Context, callID storiface.CallID, ok bool, err *storiface.CallError) error {
	return m.returnResult(callID, ok, err)
}

func (m *Manager) ReturnFetch(ctx context.Context, callID storiface.CallID, err *storiface.CallError) error {
	return m.returnResult(callID, nil, err)
}

func (m *Manager) StorageLocal(ctx context.Context) (map[stores.ID]string, error) {
	l, err := m.localStore.Local(ctx)
	if err != nil {
		return nil, err
	}

	out := map[stores.ID]string{}
	for _, st := range l {
		out[st.ID] = st.LocalPath
	}

	return out, nil
}

func (m *Manager) FsStat(ctx context.Context, id stores.ID) (fsutil.FsStat, error) {
	return m.storage.FsStat(ctx, id)
}

func (m *Manager) SchedDiag(ctx context.Context, doSched bool) (interface{}, error) {
	if doSched {
		select {
		case m.sched.workerChange <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	si, err := m.sched.Info(ctx)
	if err != nil {
		return nil, err
	}

	type SchedInfo interface{}
	i := struct {
		SchedInfo

		ReturnedWork []string
		Waiting      []string

		CallToWork map[string]string

		EarlyRet []string
	}{
		SchedInfo: si,

		CallToWork: map[string]string{},
	}

	m.workLk.Lock()

	for w := range m.results {
		i.ReturnedWork = append(i.ReturnedWork, w.String())
	}

	for id := range m.callRes {
		i.EarlyRet = append(i.EarlyRet, id.String())
	}

	for w := range m.waitRes {
		i.Waiting = append(i.Waiting, w.String())
	}

	for c, w := range m.callToWork {
		i.CallToWork[c.String()] = w.String()
	}

	m.workLk.Unlock()

	return i, nil
}

func (m *Manager) Close(ctx context.Context) error {
	return m.sched.Close(ctx)
}

var _ SectorManager = &Manager{}
