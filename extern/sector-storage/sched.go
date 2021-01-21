package sectorstorage

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle // 保存每个worker对应的处理handler

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing
}

// 每个worker的远程api和资源信息等
type workerHandle struct {
	workerRpc Worker // 远程worker的API

	info storiface.WorkerInfo // worker资源信息

	// worker的当前资源？
	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

// 发送给worker的请求
type workerRequest struct {
	sector   storage.SectorRef  // 扇区信息
	taskType sealtasks.TaskType // 任务类型
	priority int                // larger values more important	// 任务优先级
	sel      WorkerSelector     // 任务关联的worker？

	prepare WorkerAction // 任务执行之前，worker需要做的（回调函数）
	work    WorkerAction // 任务执行时，worker需要做的（回调函数）

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

// 将一个扇区的任务发送给核心的调度器
/*
sector: 扇区信息：id和扇区大小

taskType: 任务类型

*/

type WorkerPower struct {
	Hostname       string
	CurrentSectors int
	MaxSectors     int
}

var schedulerLock sync.Mutex
var sectorInWorker map[abi.SectorNumber]string = make(map[abi.SectorNumber]string, 64)

func WorkerJobs() {
	var sectorList []int

	for sectorNumber, _ := range sectorInWorker {
		sectorList = append(sectorList, int(sectorNumber))
	}

	sort.Ints(sectorList)

	for idx := range sectorList {
		sectorNumber := sectorList[idx]
		log.Debugf("^^^^^^^^ 扇区: %d  ->  Worker: [%s]\n", sectorNumber,
			sectorInWorker[abi.SectorNumber(sectorNumber)])
	}
}

var SealingWorkers []*WorkerPower

func init() {
	IntWorerList()
}

func IntWorerList() {
	SealingWorkers = append(SealingWorkers, &WorkerPower{
		"miner-node-1", 0, 8,
	})
	SealingWorkers = append(SealingWorkers, &WorkerPower{
		"worker-node-1", 0, 8,
	})
}

type ByWorkerCurrentSectors []*WorkerPower

func (a ByWorkerCurrentSectors) Len() int           { return len(a) }
func (a ByWorkerCurrentSectors) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByWorkerCurrentSectors) Less(i, j int) bool { return a[i].CurrentSectors < a[j].CurrentSectors }

func (sh *scheduler) getBestWorker(sector storage.SectorRef, taskType sealtasks.TaskType) (string, error) {
	schedulerLock.Lock()
	defer schedulerLock.Unlock()

	WorkerJobs()

	// 检查扇区任务是否被调度过
	if workerHostname, ok := sectorInWorker[sector.ID.Number]; ok {
		// 这个扇区之前的任务被调度到这台worker，那就它接下来的任务，也调度到这个worker.
		log.Debugf("^^^^^^^^ 发现扇区 [%d] 在 Worker [%v] 上做过任务，继续选择该Worker执行 任务类型 [%v]。\n",
			sector.ID.Number, workerHostname, taskType)
		return workerHostname, nil
	} else {
		// 扇区没有被调度过，寻找合适的worker
		// 寻找worker列表中，当前sectors最小的那台worker，把任务分配给它

		log.Debugf("^^^^^^^^ 扇区 [%d] 从未调度过，开始调度。\n", sector.ID.Number)
		log.Debug("^^^^^^^^ 排序前的列表: ", SealingWorkers[0], SealingWorkers[1])
		sort.Sort(ByWorkerCurrentSectors(SealingWorkers))
		minSectorWorker := SealingWorkers[0]

		log.Debug("^^^^^^^^ 排序后的列表: ", SealingWorkers[0], SealingWorkers[1])

		// 如果已经超过最大允许的并发扇区数量，就报错
		if minSectorWorker.CurrentSectors == minSectorWorker.MaxSectors {
			log.Debugf("^^^^^^^^ 发现 Worker [%v] 的任务数量已经达到最大，无法调度。\n", minSectorWorker.Hostname)
			return "", fmt.Errorf("^^^^^^^^ Scheduler reached worker MaxSectors, worker [%v]\n", minSectorWorker.Hostname)
		}

		minSectorWorker.CurrentSectors += 1
		sectorInWorker[sector.ID.Number] = minSectorWorker.Hostname

		return minSectorWorker.Hostname, nil
	}

	return "", fmt.Errorf("^^^^^^^^ !!! 调度器无法找到合适worker !!!\n")
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType,
	sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	bestWorkerName, err := sh.getBestWorker(sector, taskType)
	log.Debugf("^^^^^^^^ 调度器：获取到最优的Worker: [%v]\n", bestWorkerName)
	if err != nil {
		return err
	}

	var workerID WorkerID
	var Worker *workerHandle
	sh.workersLk.Lock()
	for {
		for wid, w := range sh.workers {
			log.Debugf("^^^^^^^^ 调度器：打印所有worker: [%v], [%v]\n", wid, w.info.Hostname)
			if w.info.Hostname == bestWorkerName {
				Worker = w
				log.Debugf("^^^^^^^^ 调度器：最优Worker [%v]　在线!\n", w.info.Hostname)
				goto Run
			}
		}

		if Worker == nil {
			log.Debugf("^^^^^^^^ 调度器：最优Worker [%v]　离线，等待Worker上线!\n", bestWorkerName)
			time.Sleep(time.Second * 5)
		}
	}

Run:
	sh.workersLk.Unlock()


	log.Debugf("^^^^^^^^ 调度器：扇区 [%v] 任务类型 [%v] 已经调度到 Worker [%v]上执行。\n",
		sector.ID.Number, taskType, Worker.info.Hostname)

	workFunc := func(ret chan workerResponse) {
		err := prepare(context.TODO(), sh.workTracker.worker(workerID, Worker.workerRpc))
		if err != nil {
			log.Errorf("^^^^^^^^ Scheduler: prepare sector: [%] type: [%v] on worker [%v] faield: [%v]\n",
				sector.ID, taskType, Worker.info.Hostname, err)
			ret <- workerResponse{err: err}
		}

		err = work(context.TODO(), sh.workTracker.worker(workerID, Worker.workerRpc))
		if err != nil {
			log.Errorf("^^^^^^^^ Scheduler: work sector: [%] type: [%v] on worker [%v] faield: [%v]\n",
				sector.ID, taskType, Worker.info.Hostname, err)
		}

		ret <- workerResponse{err: err}
	}

	log.Debugf("^^^^^^^^ Schedule Worker[%v] for sector[%v]\n", Worker.info.Hostname, sector)

	go workFunc(ret)

	select {
	case resp := <-ret:
		return resp.err
	}

	return nil
}

/*
func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType,
	sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{	// 需要worker执行的任务，发送给channel: sh.schedule
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}
*/

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

// 调度循环：持续运行
func (sh *scheduler) runSched() {
	defer close(sh.closed)

	iw := time.After(InitWait) // 等待3秒
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule: // 收到worker任务
			sh.schedQueue.Push(req) // 将任务发送到请求队列中
			doSched = true          // 标志为：需要调度

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		// 如果需要调度，并且是第一次初始化
		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
			// 首先收集所有待处理的任务，因此我们为每个添加的任务执行一次调度循环
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			// worker的调度器停止后，worker负责的任务，都需要重新加入到调度队列中.
			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched() // 调度worker
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock() // 锁定调度器全局锁
	defer sh.workersLk.RUnlock()

	windowsLen := len(sh.openWindows)
	queuneLen := sh.schedQueue.Len() // 请求队列长度

	log.Debugf("SCHED %d queued; %d open windows", queuneLen, windowsLen)

	// 如果没有worker则不需要调度直接返回
	if windowsLen == 0 || queuneLen == 0 {
		// nothing to schedule on
		return
	}

	windows := make([]schedWindow, windowsLen)
	acceptableWindows := make([][]int, queuneLen)

	// Step 1
	throttle := make(chan struct{}, windowsLen) // 根据窗口大小创建 throttle

	var wg sync.WaitGroup
	wg.Add(queuneLen)
	for i := 0; i < queuneLen; i++ { // 根据请求队列大小循环
		throttle <- struct{}{}

		go func(sqi int) { // 启动goroutine
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]                                  // 从请求队列中取出任务
			needRes := ResourceTable[task.taskType][task.sector.ProofType] // 根据扇区大小和任务类型，得出需要的资源大小

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				if !worker.enabled { // 如果worker被禁止
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Errorf("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	rmQueue := make([]int, 0, queuneLen)

	for sqi := 0; sqi < queuneLen; sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][task.sector.ProofType]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.ID.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.ID.Number, task.taskType, wnd)

			windows[wnd].allocated.add(wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
