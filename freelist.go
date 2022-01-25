package bolt

import (
	"fmt"
	"sort"
	"unsafe"
)

type freelist struct {
	// 已经可以被分配的空闲页
	ids []pgid
	// 将来很快被释放的空闲页 部分事务可能在读或者写
	pending map[txid][]pgid // mapping of soon-to-be free ids by tx
	cache   map[pgid]bool   // 记录所有 空闲或者即将空闲的 pgid
}

// newfreelista 返回一个空的初始化过的freelist
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// 返回作为 page.ptr(freelist Ver.) 的大小
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		n++ // 第一个位置用来记录freelist的统计值
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

func (f *freelist) free_count() int {
	return len(f.ids)
}

func (f *freelist) pending_count() (count int) {
	for k := range f.pending {
		count += len(f.pending[k])
	}
	return
}

// 合并 f.ids 和 f.pending中的所有pgids
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	// 首先把pending状态的页放到一个数组中，并使其有序
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	// 合并两个有序的空闲列表，最后结果输出到dst中
	mergepgids(dst, f.ids, m)
}

// 开始分配一段连续的n个页。其中返回值为初始的页id。如果无法分配，则返回0即可
// [5,6,7,(13,14,15,16),18,19,20,31,32] n == 4
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}
	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Errorf("invalid page allocation: %d", id))
		}

		// 第一次不连续时记录一下第一个位置
		if previd == 0 || id-previd != 1 {
			initial = id
		}
		// 找到了连续的块，然后将其返回即可
		if (id-initial)+1 == pgid(n) {
			if (i + 1) == n {
				// 找到的是前n个连续的空间
				f.ids = f.ids[i+1:]
			} else {
				// 更新ids的内容
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}
			// 同时更新缓存
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}
			return initial
		}
		previd = id
	}
	return 0
}

// free 解除 txid 的page及其overflow的占用 标志它们随时可以被使用 不允许二次释放
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("connot free page 0 or 1: %d", p.id))
	}

	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending[txid] = ids
}

// 将 <= txid的事务占用的page全部释放到 f的空闲列表中
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// rollback 移除 txid的所有page 并且不回收到空闲列表 仿佛没有存在过
func (f *freelist) rollback(txid txid) {
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	delete(f.pending, txid)
}

func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// 从磁盘中读取空闲页信息 并转换为freelist结构
// 转换时，也需要注意其空闲页的个数的判断逻辑
// 当p.count为0xFFFF时，需要读取p.ptr中的第一个字节来计算其空闲页的个数
// 否则则直接读取p.ptr中存放的数据为空闲页ids列表
func (f *freelist) read(p *page) {
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		// 用第一个uint64来存储整个count的值
		count = int((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr))[0])
	}

	// 这里已经取出真实的count
	if count == 0 {
		f.ids = nil // 没有空闲page
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids) // 拷贝一份空闲pgid到当前freelist中

		// 确保freelist中的pgid是有序的
		sort.Sort(pgids(f.ids))
	}
	// Rebuild the page cache
	f.reindex()

}

// 将freelist信息写入到page中
func (f *freelist) write(p *page) error {
	// Update the header flag
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		// 有溢出的情况下 后一个元素放置其长度 然后在存放所有的pgid列表
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		// 从第一个位置拷贝
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}
	return nil
}

// reload 从页面读取空闲列表并过滤掉待处理的项目。
func (f *freelist) reload(p *page) {
	f.read(p)

	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	// 一旦freeList被重建，就重建 freeCache，使其包括可用和待处理的空闲页面。
	f.reindex()

}

func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}

	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
