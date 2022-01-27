package bolt

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// 同一时间有且只能有一个读写事务执行；
// 但同一个时间可以允许有多个只读事务执行
// 每个事务都拥有自己的一套一致性视图

//提到事务，我们不得不提大家烂熟于心的事务四个特性：ACID。为方便阅读后续的内容，下面再简单回顾一下：
// A(atomic)原子性: 只要事务一开始(Begin)，那么事务要么执行成功(Commit)，要么执行失败(Rollback)。
//                  上述过程只会出现两种状态，在事务执行过程中的中间状态以及数据是不可见的。
// C(consistency)一致性：事务开始前和事务提交后的数据都是一致的。
// I(isolation)隔离性: 不同事务之间是相互隔离、互不影响的。具体的隔离程度是由具体的事务隔离级别来控制。
// D(duration)持久性: 事务开始前和事务提交后的数据都是永久的。不会存在数据丢失或者篡改的风险。

// 首先boltdb是一个文件数据库，所有的数据最终都保存在文件中。当事务结束(Commit)时，会将数据进行刷盘
// 同时，boltdb通过冗余一份元数据来做容错。当事务提交时，如果写入到一半机器挂了，此时数据就会有问题
// 而当boltdb再次恢复时，会对元数据进行校验和修复。这两点就保证事务中的持久性

// 其次boltdb在上层支持多个进程以只读的方式打开数据库，一个进程以写的方式打开数据库
// 在数据库内部中事务支持两种，读写事务和只读事务。这两类事务是互斥的
// 同一时间可以有多个只读事务执行，或者只能有一个读写事务执行，
// 上述两类事务，在底层实现时，都是保留一整套完整的视图和元数据信息，彼此之间相互隔离。因此通过这两点就保证了隔离性

// 在boltdb中，数据先写内存，然后再提交时刷盘。如果其中有异常发生，事务就会回滚
// 同时再加上同一时间只有一个进行对数据执行写入操作。所以它要么写成功提交、要么写失败回滚。也就支持原子性了

// 通过以上的几个特性的保证，最终也就保证了一致性

type txid uint64

// Tx 主要封装了读事务和写事务。其中通过writable来区分是读事务还是写事务
type Tx struct {
	writable       bool
	managed        bool
	db             *DB // 事务所属的 DB对象
	meta           *meta
	root           Bucket // 事务持有的 Bucket
	pages          map[pgid]*page
	stats          TxStats
	commitHandlers []func() // 提交时执行的动作

	// WriteFlag specifies the flag for write-related methods like WriteTo().
	// Tx opens the database file with the specified flag to copy the data.
	//
	// By default, the flag is unset, which works well for mostly in-memory
	// workloads. For databases that are much larger than available RAM,
	// set the flag to syscall.O_DIRECT to avoid trashing the page cache.
	WriteFlag int
}

func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil // 暂时没有任何page缓存

	// 复制 meta page 因为他可以被 write 所改写
	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	// 复制 root bucket
	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	// 增加事务 id 并为可写事务添加页面缓存
	if tx.writable {
		tx.pages = make(map[pgid]*page)
		tx.meta.txid += txid(1)
	}
}

func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

func (tx *Tx) DB() *DB {
	return tx.db
}

// Size 返回此事务所见的当前数据库大小（以字节为单位）
func (tx *Tx) Size() int64 {
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

func (tx *Tx) Writable() bool {
	return tx.writable
}

func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.root.CreateBucketIfNotExists(name)
}

func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.root.DeleteBucket(name)
}

// ForEach 为 root 中的每个桶执行一个函数
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.root.ForEach(func(k, v []byte) error {
		if err := fn(k, tx.root.Bucket(k)); err != nil {
			return err
		}
		return nil
	})
}

// OnCommit 添加了事务成功提交后要执行的处理函数
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit()方法内部实现中，总体思路是：
// 1. 先判定节点要不要合并、分裂
// 2. 对空闲列表的判断，是否存在溢出的情况，溢出的话，需要重新分配空间
// 3. 将事务中涉及改动的页进行排序(保证尽可能的顺序IO)，排序后循环写入到磁盘中，最后再执行刷盘
// 4. 当数据写入成功后，再将元信息页写到磁盘中，刷盘以保证持久化
// 5. 上述操作中，但凡有失败，当前事务都会进行回滚
func (tx *Tx) Commit() error {
	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	// 删除时，进行平衡，页合并
	var startTime = time.Now()
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}

	// 页分裂
	startTime = time.Now()
	// 这个内部会往缓存tx.pages中加page
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	// Free the old root bucket
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid

	// 分配新的页面给freelist，然后将freelist写入新的页面
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	// 空闲列表可能会增加，因此需要重新分配页用来存储空闲列表
	// 因为在开启写事务的时候，有去释放之前读事务占用的页信息，因此此处需要判断是否freelist会有溢出的问题
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.rollback()
		return err
	}
	// 将freelist写入到连续的新页中
	if err := tx.db.freelist.write(p); err != nil {
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// 在allocate中有可能会更改 meta.pgid ：
	// 如果不是从freelist中找到的空间的话，更新meta的id，也就意味着是从文件中新扩展的页
	// db.rwtx.meta.pgid += pgid(count)
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// If strict mode is enabled then perform a consistency check.
	// Only the first consistency error is reported in the panic.
	if tx.db.StrictMode {
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// Write dirty pages to disk.
	startTime = time.Now()
	// 写数据 根据配置 这里可能已经刷盘了
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// Write meta to disk.
	// 元信息写入到磁盘
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)
	// Finalize the transaction.
	tx.close()
	// Execute commit handlers now that the locks have been removed.
	for _, fn := range tx.commitHandlers {
		fn()
	}
	return nil

}

// Rollback()中，主要对不同事务进行不同操作：
// 1. 如果当前事务是只读事务，则只需要从db中的txs中找到当前事务，然后移除掉即可。
// 2. 如果当前事务是读写事务，则需要将空闲列表中和该事务关联的页释放掉，同时重新从freelist中加载空闲页。
func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

// 对于 RWTx 删除脏页 然后关闭事务
// 对于 RdTx 直接关闭该事务
// TODO 用这个函数测试一下 freelist 机制
func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// 移除该事务关联的pages
		tx.db.freelist.rollback(tx.meta.txid)
		// 重新从freelist页中读取构建空闲列表
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// Grab freelist stats.
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()
		// Remove transaction ref & writer lock.
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()
		// Merge statistics.
		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		// 只读事务
		tx.db.removeTx(tx)
	}
	// Clear all references.
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo 将整个数据库写入 writer
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// Attempt to open reader with WriteFlag
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Generate a meta page. We use the same page data for both meta pages.
	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta

	// Write meta 0.
	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 0 copy: %s", err)
	}

	// Write meta 1 with a lower transaction id.
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// Move past the meta pages in the file.
	// 跳到两个 meta page 之后
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// Copy data pages.
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()

}

func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	err = tx.Copy(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

func (tx *Tx) check(ch chan error) {
	// Check if any pages are double freed.
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// Track every reachable page.
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

	// Recursively check buckets.
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// Ensure all pages below high water mark are either reachable or freed.
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// Close the channel to signal completion.
	close(ch)
}

func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// Ignore inline buckets.
	if b.root == 0 {
		return
	}

	// Check every page used by this bucket.
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// Ensure each page is only referenced once.
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// We should only encounter un-freed leaf and branch pages.
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// Check each bucket within this bucket.
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache
	tx.pages[p.id] = p

	// 更新 统计值
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// 将 Tx 看到的 DB 落盘(有需要的话)
func (tx *Tx) write() error {
	// Sort pages by id.
	// 保证写的页是有序的
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	// Clear out page cache early.
	tx.pages = make(map[pgid]*page)
	sort.Sort(pages)
	// Write pages to disk in order.
	for _, p := range pages {
		// 页数和偏移量
		size := (int(p.overflow) + 1) * tx.db.pageSize
		offset := int64(p.id) * int64(tx.db.pageSize)
		// Write out page in "max allocation" sized chunks.
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		// 循环写某一页
		for {
			// Limit our write to our max allocation size.
			sz := size
			// 2^31=2G
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}
			// Write chunk to disk.
			buf := ptr[:sz]
			// 注意 tx.db.ops.writeAt = db.file.WriteAt 将 buf中的内容持续写到 db.file 中
			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}
			// Update statistics.
			tx.stats.Write++
			// Exit inner for loop if we've written all the chunks.
			size -= sz
			if size == 0 {
				break
			}
			// Otherwise move offset forward and move pointer to next chunk.
			// 移动偏移量
			offset += int64(sz)
			// 同时指针也移动
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}
	// Ignore file sync if flag is set on DB.
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}
	// Put small pages back to page pool.
	// 将没有 overflow 的 page 放回到 pagePool里
	for _, p := range pages {
		// Ignore page sizes over 1 page.
		// These are allocated using make() instead of the page pool.
		if int(p.overflow) != 0 {
			continue
		}
		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]
		// See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		// 清空buf，然后放入pagePool中
		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}
	return nil
}

func (tx *Tx) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	// 将事务的元信息写入到页中
	tx.meta.write(p)

	// Write the meta page to file.
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// Update statistics.
	tx.stats.Write++

	return nil

}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary buffered page is returned.
func (tx *Tx) page(id pgid) *page {
	// Check the dirty pages first.
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return tx.db.page(id)
}

// forEachPage iterates over every page within a given page and executes a function.
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// Page returns page information for a given page number.
// This is only safe for concurrent use when used by a writable transaction.
func (tx *Tx) Page(id int) (*PageInfo, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// Build the page info.
	p := tx.db.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// Determine the type (or if it's free).
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats represents statistics about the actions performed by the transaction.
type TxStats struct {
	// Page statistics.
	PageCount int // number of page allocations
	PageAlloc int // total bytes allocated

	// Cursor statistics.
	CursorCount int // number of cursors created

	// Node statistics
	NodeCount int // number of node allocations
	NodeDeref int // number of node dereferences

	// Rebalance statistics.
	Rebalance     int           // number of node rebalances
	RebalanceTime time.Duration // total time spent rebalancing

	// Split/Spill statistics.
	Split     int           // number of nodes split
	Spill     int           // number of nodes spilled
	SpillTime time.Duration // total time spent spilling

	// Write statistics.
	Write     int           // number of writes performed
	WriteTime time.Duration // total time spent writing to disk
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// Sub calculates and returns the difference between two sets of transaction stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
