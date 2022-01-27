package bolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeysPerPage = 2

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

// 首先 page 是一个内存结构 负责连接 磁盘文件 与 各种结构化的持久数据
// page [id 8B|flags 2B|count 2B|overflow 4B|ptr XB] Size == 4K 与内存物理页相同大小
type page struct {
	id       pgid    // 页id
	flags    uint16  // 页类型，可以是分支，叶子节点，元信息，空闲列表
	count    uint16  // 统计当前 page的ptr中有多少元素?
	overflow uint32  // 数据是否有溢出，主要在空闲列表上有用
	ptr      uintptr // 真实的数据
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// 返回 leafPageElem 的首地址
func (p *page) leafPageElement(index uint16) *leafPageElement {
	return &((*[maxAllocSize]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// 返回整个 leafPageElem 的数组
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[maxAllocSize]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[maxAllocSize]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[maxAllocSize]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:n]
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// Branch(分支) page ptr中的一种元素 leaf 由下文的leafPageElement构成
// [branchElem1 XB|brachElem2 XB|brachElem2 XB|...|brachElemn XB|k1k2|k3|...|kn]

// page ptr中的一种元素leaf的构成元素 [pos 4B|ksize 4B|pgid 8B]
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// Leaf page ptr中的一种元素 leaf 由下文的leafPageElement构成
// [leafElem1 XB|leafElem2 XB|leafElem2 XB|...|leafElemn XB|k1|v1|k2|v2|k3|v3|...|kn|vn]

// page ptr中的一种元素leaf的构成元素 [flags 4B|pos 4B|ksize 4B|visze 4B]
type leafPageElement struct {
	// 0: 叶子节点是普通的KV
	// 1: 叶子节点为桶类型，key是桶的key 当桶的元素很少的时候 value会填充为桶的pgid以及内联的kv节点数据
	flags uint32
	pos   uint32 // KV在"leaf数组"中的元素头偏移
	ksize uint32
	vsize uint32
}

func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge 有序合并两个 []pgid
//	a := []int{1, 3, 5, 7, 9, 21}
//	b := []int{2, 4, 6, 8, 10}
//	c := merge(a, b)
//  c = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 21}
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
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
