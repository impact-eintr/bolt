package bolt

import (
	"fmt"
	"unsafe"
)

type pgid int64

// 磁盘数据结构
type page struct {
	// 页id 8byte
	id pgid
	// flags 页类型 可以是分支 叶子节点 叶子节点 空闲列表 2byte
	flags uint16
	// 个数 2byte 统计叶子节点 非叶子节点 空闲列表的个数
	count uint16
	// 4byte 数据是否有溢出 主要用于空闲列表
	overflow uint32
	// 真实的数据
	ptr uintptr
}

const (
	branchPageFlag   = 0x01 // 0001
	leafPageFlag     = 0x02 // 0010
	metaPageFlag     = 0x04 // 0100
	freelistPageFlag = 0x10 // 1010
)

const (
	bucketLeafFlag = 0x01 // 0001
)

// 页头的大小 16byte
const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const minKeyPerPage = 2

// 分支节点页中每个元素所占的大小
const branchPageElementSize = int(unsafe.Sizeof(branchPagElement{}))

// 叶子节点页中每个元素所占的大小
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

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
	return fmt.Sprintf("unknow<%02d>", p.flags)
}

// page to meta(读取元数据)
func (p *page) meta() *meta {
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// 分支节点主要用来构建索引，方便提升查询效率
type branchPageElement struct {
	pos   uint32 // 该元信息和真实key之间的偏移量
	ksize uint32
	pgid  pgid
}

// key return a byte slice of the node key
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	// pos ~ ksize
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

func (p *page) branchPageElements() []branchPageElement {
	// 如果没有任何节点
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

type leafPageElement struct {
	// 该值主要用来区分 是 子桶叶子节点元素 还是普通的 key/value叶子节点元素
	// flag值为1时表示子桶 否则key/value
	flags uint32
	pos   uint32 // 该元信息和真实key之间的偏移量
	ksize uint32
	vsize uint32
}

// 叶子节点的key
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// 叶子节点的value
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

func (p *page) leafPageElement(index uint16) *leafPageElement {
	return &((*[0x7FFFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}
