package bolt

import (
	"fmt"
	"hash/fnv"
	"unsafe"
)

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

// meta to page(元数据落盘)
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	// 指定页id 和 页类型
	p.id = pgid(m.txid % 2) // 0 / 1
	p.flags |= metaPageFlag // 不清空内容的吗?

	// 计算校验和
	m.checksum = m.sum64()

	// 这里p.meta() 返回的是p.ptr的地址 因此通过copy之后 meta的信息就放到page中了
	m.copy(p.meta())
}

func (m *meta) copy(dest *meta) {
	*dest = *m
}

func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// page to meta(读取元数据)
// 见page.go 中的 p.meta()

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
