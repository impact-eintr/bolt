package bolt

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB

// The data file format version.
const version = 2

const magic uint32 = 0x514AA114 // 好臭的 magic

// IgnoreNoSync specifies whether the NoSync field of a DB is ignored when
// syncing changes to a file.  This is required as some operating systems,
// such as OpenBSD, do not have a unified buffer cache (UBC) and writes
// must be synchronized using the msync(2) syscall.
const IgnoreNoSync = runtime.GOOS == "openbsd"

// Default values if not set in a DB instance.
const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	// 16k
	DefaultAllocSize = 16 * 1024 * 1024
)

// default page size for db is set to the OS page size.
var DefaultPageSize = os.Getpagesize()

// DB represents a collection of buckets persisted to a file on disk.
// All data access is performed through transactions which can be obtained through the DB.
// All the functions on DB will return a ErrDatabaseNotOpen if accessed before Open() is called.
type DB struct {
	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	StrictMode bool
	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	NoSync bool
	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool
	// If you want to read the entire database fast, you can set MmapFlag to
	// syscall.MAP_POPULATE on Linux 2.6.23+ for sequential read-ahead.
	MmapFlags int
	// MaxBatchSize is the maximum size of a batch. Default value is
	// copied from DefaultMaxBatchSize in Open.
	//
	// If <=0, disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchSize int
	// MaxBatchDelay is the maximum delay before a batch starts.
	// Default value is copied from DefaultMaxBatchDelay in Open.
	//
	// If <=0, effectively disables batching.
	//
	// Do not change concurrently with calls to Batch.
	MaxBatchDelay time.Duration
	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	AllocSize int
	path      string
	file      *os.File // 真实存储数据的磁盘文件
	lockfile  *os.File // windows only
	dataref   []byte   // mmap'ed readonly, write throws SEGV
	// 通过mmap映射进来的地址
	data   *[maxMapSize]byte
	datasz int
	filesz int // current on disk file size
	// 元数据
	// TODO 双meta ？
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool
	rwtx     *Tx       // 写事务锁
	txs      []*Tx     // 读事务数组
	freelist *freelist // 空闲列表
	stats    Stats
	pagePool sync.Pool
	batchMu  sync.Mutex
	batch    *batch
	rwlock   sync.Mutex   // Allows only one writer at a time.
	metalock sync.Mutex   // Protects meta page access.
	mmaplock sync.RWMutex // Protects mmap access during remapping.
	statlock sync.RWMutex // Protects stats access.
	ops      struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}
	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool
}

func (db *DB) Path() string {
	return db.path
}

func (db *DB) GoString() string {
	return fmt.Sprintf("bolt.DB{path.%q}", db.path)
}

func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open()创建数据库接口
// Open()方法主要用来创建一个boltdb的DB对象，底层会执行新建或者打开存储数据的文件
// 当指定的文件不存在时， boltdb就会新建一个数据文件。否则的话，就直接加载指定的数据库文件内容
// 值的注意是，boltdb会根据Open时，options传递的参数来判断到底加互斥锁还是共享锁
// 新建时： 会调用init()方法，内部主要是新建一个文件
//          然后第0页、第1页写入元数据信息；
//          第2页写入freelist信息；
//          第3页写入bucket leaf信息。
//          并最终刷盘
// 加载时： 会读取第0页内容，也就是元信息。
//          然后对其进行校验和校验，当校验通过后获取pageSize。
//          否则的话，读取操作系统默认的pagesize(一般4k)
// 上述操作完成后，会通过mmap来映射数据。最后再根据磁盘页中的freelist数据初始化db的freelist字段
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	if options == nil {
		options = DefaultOptions
	}

	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultPageSize

	flag := os.O_RDWR // 默认开启读写事务
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	db.path = path
	var err error
	// 打开db文件
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// 只读加共享锁、否则加互斥锁
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// default values for test hooks
	db.ops.writeAt = db.file.WriteAt

	// Initialize the database if it doesn't exist.
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// Initialize new files with meta pages
		// 初始化新db文件
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// Read the first meta page to determine the page size
		// 不是新文件，读取第一页元数据 0x1000 == 4K(2 ^12)
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			// db.pageInBuffer(b, 0) 从b中读取第0页
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize()
			} else {
				// 仅仅是读取了pageSize 来初始化 db.pageSize
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// Initialize page pool
	db.pagePool = sync.Pool{
		New: func() interface{} {
			// 4k
			return make([]byte, db.pageSize)
		},
	}

	// Memory map the data file.
	// mmap 建立 db.data 与 db.file 的映射
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// Read in the freelist
	db.freelist = newFreelist()
	// db.meta().freelist=2
	// 读第二页的数据
	// 然后建立起freelist中
	db.freelist.read(db.page(db.meta().freelist))

	return db, nil

}

// mmap opens the underlying memory-mapped file and initializes the meta references.
// minsz is the minimum size that the new mmap can be.
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}

	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	if db.rwtx != nil {
		db.rwtx.root.dereference() // TODO 这是在干什么
	}

	// 解除以前的映射
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice
	// 建立新的映射
	if err := mmap(db, size); err != nil {
		return err
	}

	// 获取元数据信息
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}
	return nil
}

func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// mmapSize determines the appropriate size for the mmap given the current size
// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
// Returns an error if the new mmap size is greater than the max allowed.
// 如果大于 1GB，则一次性对齐到 1G 的倍数 1400MB => 2048MB
func (db *DB) mmapSize(size int) (int, error) {
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil // 32 KB ~ 1 GB
		}
	}

	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// 如果大于 1GB，则一次性对齐到 1G 的倍数 1400MB => 2048MB
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}
	// 确保 mmap 大小是页面大小的倍数
	// 这应该总是正确的，因为我们以 MB 为单位递增
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

func (db *DB) init() error {
	db.pageSize = os.Getpagesize()

	// 0 1 元数据
	// 2 freelist
	// 3 leaf

	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		// 读取 第 0 1 号 page
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag // 第0页和第1页存放元数据

		// 初始化 metaPage
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// Write an empty freelist at page 3.
	// 拿到第2页存放freelist
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// 第3页 存放叶子page
	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// Write the buffer to our data file.
	// 写入以上4页的数据
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	// 刷盘
	if err := fdatasync(db); err != nil {
		return err
	}
	return nil
}

func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	// Clear ops.
	db.ops.writeAt = nil

	// Close the mmap.
	if err := db.munmap(); err != nil {
		return err
	}

	// Close file handles.
	if db.file != nil {
		// No need to unlock read-only file.
		if !db.readOnly {
			// Unlock the file.
			if err := funlock(db); err != nil {
				log.Printf("bolt.Close(): funlock error: %s", err)
			}
		}

		// Close the file descriptor.
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin()开启事务接口
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) beginTx() (*Tx, error) {
	db.metalock.Lock()
	db.mmaplock.RLock() // 这句是关键 如果顺利 这个锁不会解除 也就是给mmap上了个共享读锁
	// Exit if the database is not open yet.
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}
	// Create a transaction associated with the database.
	t := &Tx{}
	t.init(db)
	// Keep track of transaction until it closes.
	db.txs = append(db.txs, t)
	n := len(db.txs)
	// Unlock the meta pages.
	db.metalock.Unlock()
	// Update the transaction stats.
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()
	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// If the database was opened with Options.ReadOnly, return an error.
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}
	// Obtain writer lock. This is released by the transaction when it closes.
	// This enforces only one writer transaction at a time.
	db.rwlock.Lock() // 这句是关键 如果顺利 这个锁不会解除 也就是给 db.rwtx(一个 *Tx) 上了个互斥锁
	// Once we have the writer lock then we can lock the meta pages so that
	// we can set up the transaction.
	db.metalock.Lock()
	defer db.metalock.Unlock()
	// Exit if the database is not open yet.
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}
	// Create a transaction associated with the database.
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t // 将新创建的读事务注册到 db.rwtx 中
	// Free any pages associated with closed read-only transactions.
	var minid txid = 0xFFFFFFFFFFFFFFFF
	// 找到最小的事务id
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		// 将之前事务关联的page全部释放了，因为在只读事务中，没法释放只读事务的页，
		// 因为可能当前的事务已经完成 ，但实际上其他的读事务还在用
		db.freelist.release(minid - 1)
	}
	return t, nil
}

func (db *DB) removeTx(tx *Tx) {
	// Release the read lock on the mmap.
	db.mmaplock.RUnlock()

	// Use the meta lock to restrict access to the DB object.
	db.metalock.Lock()

	// Remove the transaction.
	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// Unlock the meta pages.
	db.metalock.Unlock()

	// Merge statistics.
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()

}

// Update()更新接口
func (db *DB) Update(fn func(*Tx) error) error {
	// 开启一个 读写事务
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true
	err = fn(t)
	t.managed = false

	// 执行失败
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()

}

// View()查询接口
func (db *DB) View(fn func(*Tx) error) error {
	// 开启一个只读事务
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	t.managed = true
	err = fn(t)
	t.managed = false

	// 查询失败
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil

}

// 批量操作
// 一个DB对象拥有一个batch对象，该对象是全局的
// 当我们使用Batch()方法时，内部会对将传递进去的fn缓存在calls中
// 其内部也是调用了Update，只不过是在Update内部遍历之前缓存的calls
//
// 有两种情况会触发调用Update
// 1. 到达了MaxBatchDelay时间，就会触发Update
// 2. len(db.batch.calls) >= db.MaxBatchSize，即缓存的calls个数大于等于MaxBatchSize时，也会触发Update

// Batch()批量更新接口
// Batch的本质是： 将每次写、每次刷盘的操作转变成了多次写、一次刷盘，从而提升性能
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)
	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// There is no existing batch, or the existing batch is full; start a new one.
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// wake up batch, it's ready to run
		go db.batch.trigger()
	}
	db.batchMu.Unlock()
	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}
type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger runs the batch if it hasn't already been run.
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run performs the transactions in the batch and communicates results
// back to DB.Batch.
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// Make sure no new work is added to this batch, but don't break
	// other batches.
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()
retry:
	// 内部多次调用Update，最后一次Commit刷盘，提升性能
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			// 遍历calls中的函数c，多次调用，最后一次提交刷盘
			for i, c := range b.calls {
				// safelyCall里面捕获了panic
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					//只要又失败，事务就不提交
					return err
				}
			}
			return nil
		})
		if failIdx >= 0 {
			// take the failing transaction out of the batch. it's
			// safe to shorten b.calls here because db.batch no longer
			// points to us, and we hold the mutex anyway.
			c := b.calls[failIdx]
			//这儿只是把失败的事务给踢出去了，然后其他的事务会重新执行
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// tell the submitter re-run it solo, continue with the rest of the batch
			c.err <- trySolo
			continue retry
		}
		// pass success, or bolt internal errors, to all callers
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

// trySolo is a special sentinel error value used for signaling that a
// transaction function should be re-run. It should never be seen by
// callers.
var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}
func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync executes fdatasync() against the database file handle.
//
// This is not necessary under normal operation, however, if you use NoSync
// then it allows you to force the database file to sync against the disk.
func (db *DB) Sync() error { return fdatasync(db) }

// Stats retrieves ongoing performance stats for the database.
// This is only updated when a transaction closes.
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// This is for internal access to the raw data bytes from the C cursor, use
// carefully, or not at all.
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer 根据当前 pageSize 从给定的 []byte  b中检索页面引用
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta retrieves the current meta page reference.(返回最新的事务)
func (db *DB) meta() *meta {
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}
	panic("bolt.DB.meta(): invalid meta pages")
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	// 转成*page
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// 先从空闲列表中找
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// p.id = 0
	// 找不到的话，就按照事务的pgid来分配
	// 表示需要从文件内部扩大
	p.id = db.rwtx.meta.pgid
	// 因此需要判断是否目前所有的页数已经大于了mmap映射出来的空间
	// 这儿计算的页面总数是从当前的id后还要计算count+1个
	var minsz = int((p.id + pgid(count) + 1)) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	// 如果不是从freelist中找到的空间的话，更新meta的id，也就意味着是从文件中新扩展的页
	db.rwtx.meta.pgid += pgid(count)
	return p, nil
}

// grow 将数据库的大小增加到给定的 sz
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}
	// 满足这个条件sz>filesz
	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}
	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}
	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

type Options struct {
	// Timeout is the amount of time to wait to obtain a file lock.
	// When set to zero it will wait indefinitely. This option is only
	// available on Darwin and Linux.
	Timeout time.Duration

	// Sets the DB.NoGrowSync flag before memory mapping the file.
	NoGrowSync bool

	// Open database in read-only mode. Uses flock(..., LOCK_SH |LOCK_NB) to
	// grab a shared lock (UNIX).
	ReadOnly bool

	// Sets the DB.MmapFlags flag before memory mapping the file.
	MmapFlags int

	// InitialMmapSize is the initial mmap size of the database
	// in bytes. Read transactions won't block write transaction
	// if the InitialMmapSize is large enough to hold database mmap
	// size. (See DB.Begin for more information)
	//
	// If <=0, the initial map size is 0.
	// If initialMmapSize is smaller than the previous database size,
	// it takes no effect.
	InitialMmapSize int
}

// DefaultOptions represent the options used if nil options are passed into Open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

// Stats represents statistics about the database.
type Stats struct {
	// Freelist stats
	FreePageN     int // total number of free pages on the freelist
	PendingPageN  int // total number of pending pages on the freelist
	FreeAlloc     int // total bytes allocated in free pages
	FreelistInuse int // total bytes used by the freelist

	// Transaction stats
	TxN     int // total number of started read transactions
	OpenTxN int // number of currently open read transactions

	TxStats TxStats // global, ongoing stats.
}

// Sub calculates and returns the difference between two sets of database stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

type Info struct {
	Data     uintptr
	PageSize int
}

// page ptr中的一种元素 [magic 4B|version 4B|pagesize 4B|flags 4B|root(bucket)XB|pgid 8B|txid 8B|checksum 8B]
type meta struct {
	magic    uint32 // 魔数
	version  uint32 // 版本
	pageSize uint32 // page页的大小 该值和操作系统默认的页大小保持一致
	flags    uint32 // 保留值
	root     bucket // 所有小柜子bucket的根
	freelist pgid   // 空闲列表页的id
	pgid     pgid   // 元数据页的id
	txid     txid   // 最大的事物id
	checksum uint64 // 用作校验的校验和
}

func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

func (m *meta) copy(dst *meta) {
	*dst = *m
}

// 将当前的meta 作为 page.ptr 写到page里
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}
	// TODO 事物相关 等待解析
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag // 标志这个 page 是一个 metaPage
	m.checksum = m.sum64()
	m.copy(p.meta())
}

func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
