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
	cache   map[pgid]bool   // fast lookup of all free and pending page ids
}

type pgids []pgid

// 为了可以排序需要实现的接口
func (s pgids) Len() int {
	return len(s)
}

func (s pgids) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s pgids) Less(i, j int) bool {
	return s[i] < s[j]
}

// newfreelista 返回一个空的初始化过的freelist
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
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

func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	// TODO 这里的id应该是包含pendingid的?
	for _, id := range f.ids {
		f.cache[id] = true
	}

	// TODO 搞清楚这里的f.pending有没有东西 为什么有/没有
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
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

func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	// 首先把pending状态的页放到一个数组中，并使其有序
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	// 合并两个有序的列表，最后结果输出到dst中
	// TODO 一旦准备落盘 则说明该页这些事务已经执行完了?
	mergepgids(dst, f.ids, m)
}

// 将a和b按照有序合并成到dst中，a和b有序
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil
	if len(a) == 0 {
		copy(dst, b)
		return
	}

	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}
	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}
		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}
	// Append what's left in follow.
	_ = append(merged, follow...)
}

func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		n++ // 第一个位置被用作记录count了
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

		// Reset initial page if this is not contiguous(连续)
		if previd == 0 || id-previd != 1 {
			// 第一次不连续时记录一下第一个位置
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
			// Remove from the free cache.
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

func (f *freelist) free(txid txid, p *page) {
	// TODO
}

func (f *freelist) release(txid txid) {
	// TODO
}

func (f *freelist) rollback(txid txid) {
	// TODO
}

func (f *freelist) freed(pgid pgid) bool {
	// TODO
	return f.cache[pgid]
}

func (f *freelist) reload(p *page) {
	// TODO
}
