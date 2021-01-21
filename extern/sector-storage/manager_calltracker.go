package sectorstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type WorkID struct {
	Method sealtasks.TaskType
	Params string // json [...params]
}

func (w WorkID) String() string {
	return fmt.Sprintf("%s(%s)", w.Method, w.Params)
}

var _ fmt.Stringer = &WorkID{}

type WorkStatus string

const (
	wsStarted WorkStatus = "started" // task started, not scheduled/running on a worker yet
	wsRunning WorkStatus = "running" // task running on a worker, waiting for worker return
	wsDone    WorkStatus = "done"    // task returned from the worker, results available
)

// 工作状态
type WorkState struct {
	ID WorkID	// 工作ID

	Status WorkStatus	// 工作状态：started/running/done

	WorkerCall storiface.CallID // Set when entering wsRunning
	WorkError  string           // Status = wsDone, set when failed to start work

	WorkerHostname string // 最后处理这个任务的worker名字
	StartTime      int64  // 任务开始时间：unix时间戳
}

// 根据任务类型、扇区的信息生成一个WorkID数据
func newWorkID(method sealtasks.TaskType, params ...interface{}) (WorkID, error) {
	pb, err := json.Marshal(params)
	if err != nil {
		return WorkID{}, xerrors.Errorf("marshaling work params: %w", err)
	}

	if len(pb) > 256 {
		s := sha256.Sum256(pb)
		pb = []byte(hex.EncodeToString(s[:]))
	}

	return WorkID{
		Method: method,
		Params: string(pb),
	}, nil
}

// miner启动时，设置工作跟踪器
func (m *Manager) setupWorkTracker() {
	m.workLk.Lock()
	defer m.workLk.Unlock()

	// 从manager的manager state store中读取 "工作状态" 列表
	var ids []WorkState
	if err := m.work.List(&ids); err != nil {
		log.Error("getting work IDs") // quite bad
		return
	}

	// 从任务状态存储中读取的所有任务状态
	// 循环处理
	for _, st := range ids {
		wid := st.ID

		// 如果定义了环境变量 LOTUS_MINER_ABORT_UNFINISHED_WORK，就设置任务状态是wsDone.
		if os.Getenv("LOTUS_MINER_ABORT_UNFINISHED_WORK") == "1" {
			st.Status = wsDone
		}

		switch st.Status {
		case wsStarted:	// 工作已经开始，但是没有调度/运行在任何一个worker上
			log.Warnf("dropping non-running work %s", wid)

			// 放弃未运行的任务
			if err := m.work.Get(wid).End(); err != nil {
				log.Errorf("cleannig up work state for %s", wid)
			}
		case wsDone:	// 已经从worker返回的工作，返回的结果可用？
			// can happen after restart, abandoning work, and another restart
			log.Warnf("dropping done work, no result, wid %s", wid)

			if err := m.work.Get(wid).End(); err != nil {
				log.Errorf("cleannig up work state for %s", wid)
			}
		case wsRunning:	// 工作正在运行在一个worker上，等待worker返回。
			// 在m.callToWork中记录: CallID -> 任务ID
			m.callToWork[st.WorkerCall] = wid
		}
	}
}

// returns wait=true when the task is already tracked/running
/*
manager记录每个任务（由扇区、任务类型、ticket、pieces构成）
返回任务ID，cancel函数
 */
func (m *Manager) getWork(ctx context.Context, method sealtasks.TaskType, params ...interface{}) (wid WorkID, wait bool, cancel func(), err error) {
	// 把任务的信息组成一个WorkID
	wid, err = newWorkID(method, params)
	if err != nil {
		return WorkID{}, false, nil, xerrors.Errorf("creating WorkID: %w", err)
	}

	m.workLk.Lock()
	defer m.workLk.Unlock()

	// 查询 m.work 的存储中，是否已经存在这个任务
	have, err := m.work.Has(wid)
	if err != nil {
		return WorkID{}, false, nil, xerrors.Errorf("failed to check if the task is already tracked: %w", err)
	}

	// 如果 m.work 中不存在这个任务
	// 说明是新任务，保存新任务后，返回 "不需要等待"标志和cancel函数
	if !have {
		// 在manager的datastore保存这个任务的信息（持久化任务信息）
		err := m.work.Begin(wid, &WorkState{
			ID:     wid,
			Status: wsStarted,
		})
		if err != nil {
			return WorkID{}, false, nil, xerrors.Errorf("failed to track task start: %w", err)
		}

		// 返回workID、false(不需要等待旧任务)、取消函数
		return wid, false, func() {
			/*
				取消函数的逻辑
			 */

			m.workLk.Lock()
			defer m.workLk.Unlock()

			// 在 m.work 中查询这个任务的信息
			have, err := m.work.Has(wid)
			if err != nil {	// 查询失败报错返回
				log.Errorf("cancel: work has error: %+v", err)
				return
			}

			// 如果 m.work 中这个任务没查到，说明调用cancel函数的时候，任务已经结束，可以直接返回。
			if !have {
				return // expected / happy path
			}

			/*
				查到了，说明任务还在运行，取消任务。
			 */

			var ws WorkState
			// 查询工作ID对应的工作信息
			if err := m.work.Get(wid).Get(&ws); err != nil {
				log.Errorf("cancel: get work %s: %+v", wid, err)
				return
			}

			// 根据状态决定下一步处理.
			switch ws.Status {
			// 如果任务已经在开始，但是未运行，则取消任务
			case wsStarted:
				log.Warnf("canceling started (not running) work %s", wid)

				// 根据wid取消这个任务
				if err := m.work.Get(wid).End(); err != nil {
					log.Errorf("cancel: failed to cancel started work %s: %+v", wid, err)
					return
				}
			case wsDone:
				// TODO: still remove?
				log.Warnf("cancel called on work %s in 'done' state", wid)
			case wsRunning:
				// 如果任务在m.work中不存在，但是状态是Running，就取消. 如何取消？
				log.Warnf("cancel called on work %s in 'running' state (manager shutting down?)", wid)
			}

		}, nil
	}

	// already started

	// 到这里说明任务在 m.work 中能查到记录，说明任务正在某个worker上运行，应该等待worker完成。
	return wid, true, func() {
		// TODO
	}, nil
}

func (m *Manager) startWork(ctx context.Context, w Worker, wk WorkID) func(callID storiface.CallID, err error) error {
	// 返回这个函数，接受例如 worker.PreCommit1 函数调用返回的CallID和error
	return func(callID storiface.CallID, err error) error {

		// 取得正确的worker名称
		var hostname string
		info, ierr := w.Info(ctx)
		if ierr != nil {
			hostname = "[err]"
		} else {
			hostname = info.Hostname
		}

		m.workLk.Lock()
		defer m.workLk.Unlock()

		// 如果调用例如 worker.PreCommit1 函数返回错误
		// 就设置 m.work 中任务信息为wsDone，并返回错误
		if err != nil {
			merr := m.work.Get(wk).Mutate(func(ws *WorkState) error {
				ws.Status = wsDone
				ws.WorkError = err.Error()
				return nil
			})

			if merr != nil {
				return xerrors.Errorf("failed to start work and to track the error; merr: %+v, err: %w", merr, err)
			}
			return err
		}

		// 从 m.work 根据任务ID查询到任务状态，接着回调函数中修改状态，修改后状态会被Mutate函数保存。
		err = m.work.Get(wk).Mutate(func(ws *WorkState) error {
			// 保存状态前，先查询一下结果是否返回.
			// 如果已经返回，说明worker调用了returnResult函数.
			// 并且说明之前Manager开始调度任务前的检查函数getWork并没有检查到这个任务的存在，
			// 说明miner的 m.work 中之前保存的任务丢失了.
			_, ok := m.results[wk]
			if ok {
				// 这里报警：在开始跟踪任务前，它已经返回了结果。
				log.Warn("work returned before we started tracking it")
				ws.Status = wsDone	// 设置任务状态为完成
			} else {
				ws.Status = wsRunning	// 设置任务状态为正在运行
			}
			ws.WorkerCall = callID	// 在任务状态信息中，保存callID
			ws.WorkerHostname = hostname	// 保存任务分配到的worker名
			ws.StartTime = time.Now().Unix()	// 任务开始时间
			return nil
		})

		// 保存任务状态错误，返回注册work失败。
		if err != nil {
			return xerrors.Errorf("registering running work: %w", err)
		}

		// 保存callID对应的任务ID.
		m.callToWork[callID] = wk

		return nil
	}
}

// 等待worker完成某个任务
// wid是由扇区、任务类型、扇区其他信息生成的ID
func (m *Manager) waitWork(ctx context.Context, wid WorkID) (interface{}, error) {
	m.workLk.Lock()

	// 首先在任务跟踪表中查询，是否已经存在这个任务
	var ws WorkState
	if err := m.work.Get(wid).Get(&ws); err != nil {
		// 查询失败返回错误
		m.workLk.Unlock()
		return nil, xerrors.Errorf("getting work status: %w", err)
	}

	// 如果任务处于 wsStated 状态，而不是 wsRunning 状态.
	// 报告任务状态错误并返回
	if ws.Status == wsStarted {
		m.workLk.Unlock()
		return nil, xerrors.Errorf("waitWork called for work in 'started' state")
	}

	// 完整性检查：如果m.work中保存的调用ID，在m.callToWork中查询，得到的任务ID不一致，就返回错误。
	// 哪种场景下会不一致？
	wk := m.callToWork[ws.WorkerCall]
	if wk != wid {
		m.workLk.Unlock()
		return nil, xerrors.Errorf("wrong callToWork mapping for call %s; expected %s, got %s", ws.WorkerCall, wid, wk)
	}

	// 到了等待工作完成这一步，说明已经调用了worker的封装接口，而接口也没有返回错误
	// 所以在等待worker返回任务前，先清理掉之前同样的CallID。
	// 同样的CallID会返回两次？
	cr, ok := m.callRes[ws.WorkerCall]
	if ok {
		delete(m.callToWork, ws.WorkerCall)

		if len(cr) == 1 {
			err := m.work.Get(wk).End()
			if err != nil {
				m.workLk.Unlock()
				// Not great, but not worth discarding potentially multi-hour computation over this
				log.Errorf("marking work as done: %+v", err)
			}

			res := <-cr
			delete(m.callRes, ws.WorkerCall)

			m.workLk.Unlock()
			return res.r, res.err
		}

		m.workLk.Unlock()
		return nil, xerrors.Errorf("something else in waiting on callRes")
	}

	done := func() {
		delete(m.results, wid)

		_, ok := m.callToWork[ws.WorkerCall]
		if ok {
			delete(m.callToWork, ws.WorkerCall)
		}

		err := m.work.Get(wk).End()
		if err != nil {
			// Not great, but not worth discarding potentially multi-hour computation over this
			log.Errorf("marking work as done: %+v", err)
		}
	}

	// the result can already be there if the work was running, manager restarted,
	// and the worker has delivered the result before we entered waitWork
	res, ok := m.results[wid]
	if ok {
		done()
		m.workLk.Unlock()
		return res.r, res.err
	}

	ch, ok := m.waitRes[wid]
	if !ok {
		ch = make(chan struct{})
		m.waitRes[wid] = ch
	}

	m.workLk.Unlock()

	select {
	case <-ch:
		m.workLk.Lock()
		defer m.workLk.Unlock()

		res := m.results[wid]
		done()

		return res.r, res.err
	case <-ctx.Done():
		return nil, xerrors.Errorf("waiting for work result: %w", ctx.Err())
	}
}

func (m *Manager) waitSimpleCall(ctx context.Context) func(callID storiface.CallID, err error) (interface{}, error) {
	return func(callID storiface.CallID, err error) (interface{}, error) {
		if err != nil {
			return nil, err
		}

		return m.waitCall(ctx, callID)
	}
}

func (m *Manager) waitCall(ctx context.Context, callID storiface.CallID) (interface{}, error) {
	m.workLk.Lock()
	_, ok := m.callToWork[callID]	// 在callToWork中，查找callID是否存在
	if ok {	// 如果 存在，就报告错误
		m.workLk.Unlock()
		return nil, xerrors.Errorf("can't wait for calls related to work")
	}

	ch, ok := m.callRes[callID]
	if !ok {
		ch = make(chan result, 1)
		m.callRes[callID] = ch
	}
	m.workLk.Unlock()

	defer func() {
		m.workLk.Lock()
		defer m.workLk.Unlock()

		delete(m.callRes, callID)
	}()

	select {
	case res := <-ch:
		return res.r, res.err
	case <-ctx.Done():
		return nil, xerrors.Errorf("waiting for call result: %w", ctx.Err())
	}
}

// 由worker调用，通知manager某个扇区任务完成.
func (m *Manager) returnResult(callID storiface.CallID, r interface{}, cerr *storiface.CallError) error {
	log.Debugf("^^^^^^^^ manager returnResult() called, result: [%v]\n", r)
	res := result{
		r: r,
	}
	if cerr != nil {
		res.err = cerr
	}

	// 通知 "工作跟踪器" 某次扇区任务完成
	m.sched.workTracker.onDone(callID)

	m.workLk.Lock()
	defer m.workLk.Unlock()

	// 如果 m.callToWork 中对应的CallID数据为空（miner丢失了一部分的m.work数据？)
	// (m.callToWork会在Manager启动时通过setupWorkTrace设置.)
	wid, ok := m.callToWork[callID]
	if !ok {
		rch, ok := m.callRes[callID]	// 如果这个CallID也没有worker做过doReturn
		if !ok {
			rch = make(chan result, 1)
			m.callRes[callID] = rch	// 就创建一个保存result的channel，并保存在在callRes中
		}

		if len(rch) > 0 {
			return xerrors.Errorf("callRes channel already has a response")
		}
		if cap(rch) == 0 {
			return xerrors.Errorf("expected rch to be buffered")
		}

		log.Debugf("^^^^^^^^ manager returnResult() write res to rch!\n")
		rch <- res	// 将worker return的结果放在channel中
		return nil	// 函数返回
	}

	// 如果m.results已经保存了对应workID的结果
	// 就返回错误
	_, ok = m.results[wid]
	if ok {
		return xerrors.Errorf("result for call %v already reported", wid)
	}

	// 在m.results中保存结果
	m.results[wid] = res

	// 设置work状态是wsDone，表示任务完成。
	err := m.work.Get(wid).Mutate(func(ws *WorkState) error {
		ws.Status = wsDone
		return nil
	})
	if err != nil {
		// in the unlikely case:
		// * manager has restarted, and we're still tracking this work, and
		// * the work is abandoned (storage-fsm doesn't do a matching call on the sector), and
		// * the call is returned from the worker, and
		// * this errors
		// the user will get jobs stuck in ret-wait state
		log.Errorf("marking work as done: %+v", err)
	}

	// 如果在m.waitRes中存在等待的channel，则关闭对应channel（通知对方读取数据）.
	// 重要：在manager开始调度之前，会根据WorkID检查对应扇区类型任务是否已经存在，
	// 存在就会等待旧任务，而不是创建新的调度任务。
	_, found := m.waitRes[wid]
	if found {
		close(m.waitRes[wid])
		delete(m.waitRes, wid)
	}

	return nil
}

func (m *Manager) Abort(ctx context.Context, call storiface.CallID) error {
	// TODO: Allow temp error
	return m.returnResult(call, nil, storiface.Err(storiface.ErrUnknown, xerrors.New("task aborted")))
}
