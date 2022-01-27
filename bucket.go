package bolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	MaxKeySize   = 32768
	MaxValueSize = (1 << 31) - 2
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{})) // bucket是Bucket的Header

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

const DefaultFillPercent = 0.5

// 一组key/value的集合 也就是一个b+树
// 单个Bucket的结构：
// [bucketElem | b_key | b_val ] b_val = val + page
// page : [pageHeader|leafElem1|leafElem2|...|leafElemn|k1|v1|k2|v2|...|kn|vn]
//
// 当一个桶同时满足下面两个条件的时候 视作内联
// 1. 当前的桶没有其他嵌套子桶
// 2. 当前桶内的元素所占的总字节数小于 1/4 pageSize(4K)
//
// 多个Bucket的结构：
//                                      [val1|child_inline_page1]
// [bucketElem1 | bucketElem2 | bkey_1 | b_val1 | b_key2 | b_val2]
//                                                         [val2|child_inline_page2]
type Bucket struct {
	*bucket     // 在内联时bucket主要用来存储其桶的value并在后面拼接所有的元素，即所谓的内联
	tx          *Tx
	buckets     map[string]*Bucket // subbucket cache 用于快速查找
	page        *page              // 内联页的引用
	rootNode    *node              // 每个b+树的根节点
	nodes       map[pgid]*node     // node cache 用于快速查找
	FillPercent float64            // 填充率
}

// 存储桶表示存储桶的文件表示
type bucket struct {
	root     pgid   // Bucket 的 rootPage 的 pgid
	sequence uint64 // 单调递增，由 NextSequence() 使用
}

func newBucket(tx *Tx) Bucket {
	var b = Bucket{
		tx:          tx,
		FillPercent: DefaultFillPercent,
	}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// 返回桶当前持有的事务
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// 返回桶的根节点(B+ Tree Root)
func (b *Bucket) Root() pgid {
	return b.root
}

// 判断bucket持有的事务是 读/写 事务
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// 既然一个Bucket逻辑上是一颗b+树，那就意味着我们可以对其进行遍历
// 前面提到的set、get操作，无非是要在Bucket上先找到合适的位置，然后再进行操作
// 而“找”这个操作就是交由Cursor(游标)来完成的
// 简而言之对Bucket这颗b+树的遍历工作由Cursor来执行, 一个Bucket对象关联一个Cursor。
func (b *Bucket) Cursor() *Cursor {
	// 更新事务中的 Cursor 的计数
	b.tx.stats.CursorCount++

	// Allocate and return a cursor
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket 按名称检索嵌套的存储桶
// 根据指定的key来获取一个Bucket。如果找不到则返回nil。
func (b *Bucket) Bucket(name []byte) *Bucket {
	// 如果是一个内联桶
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}
	// Move cursor to key
	// 获取当前bucket的cursor根据游标找key
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// Return nil if the key doesn't exist or it is not a bucket
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// 根据找到的value来打开桶
	var child = b.openBucket(v)
	// 把这个桶缓存起来
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}
	return child
}

func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	// TODO 不强制对齐会怎么样？
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0 // 判断value首地址是否8对齐

	if unaligned {
		value = cloneBytes(value)
	}

	if b.tx.writable && !unaligned { // 事务可写 且 已经对齐
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// 内联桶
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}
	return &child
}

// 根据 pgid 给指定的 parent 整个 child_node
// 用 key 创建 一个新桶
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}
	// 拿到游标
	c := b.Cursor()
	// 开始遍历 找到合适的位置
	k, _, flag := c.seek(key)

	if bytes.Equal(key, k) {
		// 是一个桶 已经存在了 这个桶的key正是传入的key
		if (flag & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		// 不是桶 但是key已经存在了 无法插入
		return nil, ErrIncompatibleValue
	}

	// Create a empty inline bucket
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}

	// 拿到这个新 Bucket 对应的value 包含Bucket的头信息和rootNode的头信息
	var value = bucket.write()
	// Insert into node
	key = cloneBytes(key)

	// 将 key&value 从Cursor现在的位置插入到合适的位置上
	// c.node()方法会在内存中建立这颗树 调用n.read(page)
	// c.node() -> node.ChildAt(index int)[这里有一个for] -> bucket.node(pgid, *node) ->
	//   根据pgid创建一个node -内存中没有-> node.read(page) -> 根据page来初始化node
	c.node().put(key, key, value, 0, bucketLeafFlag) // 插入子桶节点

	b.page = nil

	return b.Bucket(key), nil // 根据名字找到这个新建的桶
}

// 创建一个Bucket
// 根据指定的key来创建一个Bucket,如果指定key的Bucket已经存在，则会报错
// 如果指定的key之前有插入过元素，也会报错
// 否则的话，会在当前的Bucket中找到合适的位置，然后新建一个Bucket插入进去，最后返回给客户端
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil //存在直接返回
	} else if err != nil {
		return nil, err // 出问题了
	}
	return child, nil // 正常返回
}

// TODO 能不能删掉自己
// 删除一个Bucket
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}
	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)
	// Return an error if bucket doesn't exist or is not a bucket.
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}
	// Recursively delete all child buckets.
	child := b.Bucket(key)
	// 将该桶下面的 所有子桶 递归删除
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// 清除对应缓存
	delete(b.buckets, string(key))
	// 释放子桶对应的 page 到 freelist
	child.nodes = nil
	child.rootNode = nil
	child.free()
	// Delete the node if we have a matching key.
	c.node().del(key)
	return nil
}

// ================= key/value的插入 获取 删除 ====================
// 其实本质上，对key/value的所有操作最终都要表现在底层的node上。因为node节点就是用来存储真实数据的

// 获取一个key/value对
func (b *Bucket) Get(key []byte) []byte {
	k, v, flags := b.Cursor().seek(key)

	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// 插入一个key/value对
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	c := b.Cursor()
	k, _, flags := c.seek(key)

	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	// Insert into node
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0) // 插入普通叶子节点

	return nil
}

// 删除一个key/value对
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}
	// Move cursor to correct position.
	c := b.Cursor()
	_, _, flags := c.seek(key)
	// Return an error if there is already existing bucket value.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	// Delete the node if we have a matching key.
	c.node().del(key)
	return nil
}

func (b *Bucket) Sequence() uint64 {
	return b.sequence
}

// SetSequence 更新 Bucket 的序列号
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence = v
	return nil
}

// NextSequence 返回 Bucket 的自动递增整数
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// 遍历Bucket中所有的键值对
// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Stat returns stats on a bucket.
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			// used totals the used bytes for the page
			used := pageHeaderSize

			if p.count != 0 {
				// If page has any elements, add all element headers.
				used += leafPageElementSize * int(p.count-1)

				// Add all element key, value sizes.
				// The computation takes advantage of the fact that the position
				// of the last element's key/value equals to the total of the sizes
				// of all previous elements' keys and values.
				// It also includes the last element's header.
				lastElement := p.leafPageElement(p.count - 1)
				used += int(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}
			if b.root == 0 {
				// For inlined bucket just update the inline stats
				s.InlineBucketInuse += used
			} else {
				// For non-inlined bucket update all the leaf stats
				s.LeafPageN++
				s.LeafInuse += used
				s.LeafOverflowN += int(p.overflow)

				// Collect stats from sub-buckets.
				// Do that by iterating over all element headers
				// looking for the ones with the bucketLeafFlag.
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						// For any bucket element, open the element value
						// and recursively call Stats on the contained bucket.
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used totals the used bytes for the page
			// Add header and all element headers.
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1))

			// Add size of all keys and values.
			// Again, use the fact that last element's position equals to
			// the total of key, value sizes of all previous elements.
			used += int(lastElement.pos + lastElement.ksize)
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}
		// Keep track of maximum page depth.
		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})

	// Alloc stats can be computed from page counts and pageSize.
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// Add the max depth of sub-buckets to get total nested depth.
	s.Depth += subStats.Depth
	// Add the stats for all sub-buckets
	s.Add(subStats)
	return s
}

// forEachPage iterates over every page in a bucket, including inline pages.
func (b *Bucket) forEachPage(fn func(*page, int)) {
	// If we have an inline page then just use that.
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	// Otherwise traverse the page hierarchy.
	b.tx.forEachPage(b.root, 0, fn)
}

func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// 执行函数
	fn(p, n, depth)

	// 对当前节点的所有子节点递归执行函数
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// spill 将此 Bucket 的所有节点写入脏页
func (b *Bucket) spill() error {
	// Spill all child buckets first.
	for name, child := range b.buckets {
		// If the child bucket is small enough and it has no child buckets then
		// write it inline into the parent bucket's page. Otherwise spill it
		// like a normal bucket and make the parent value a pointer to the page.
		var value []byte
		if child.inlineable() {
			child.free()
			// 重新更新bucket的val值
			value = child.write()
		} else {
			if err := child.spill(); err != nil {
				return err
			}

			// Update the child bucket header in this bucket.
			// 记录 value
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// Skip writing the bucket if there are no materialized nodes.
		if child.rootNode == nil {
			continue
		}

		// Update parent node.
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		// 更新子桶的 value
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there's not a materialized root node.
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// Update the root node for this bucket.
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	b.root = b.rootNode.pgid

	return nil

}

// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// Bucket must only contain a single leaf node.
	if n == nil || !n.isLeaf {
		return false
	}

	// Bucket is not inlineable if it contains subbuckets or if it goes beyond
	// our threshold for inline bucket size.
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 {
			// 有子桶时 不能内联
			return false
		} else if size > b.maxInlineBucketSize() {
			// 如果长度大于 1/4 page时 不能内联
			return false
		}
	}

	return true
}

// Returns the maximum total size of a bucket to make it a candidate for inlining.
func (b *Bucket) maxInlineBucketSize() int {
	return b.tx.db.pageSize / 4
}

// 内联桶的话 其value中bucketHeaderSize后面的内容为其page的数据
func (b *Bucket) write() []byte {
	// Allocate the appeopriate(恰当的) size
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// Write a bucket header
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	// 将该桶中的元素压缩 放在value中
	n.write(p)

	return value
}

// Bucket 尝试平衡所有节点
func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}

	for _, child := range b.buckets {
		child.rebalance()
	}
}

func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// 如果node已经存在 检索node
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// 否则创建一个新的node并缓存
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}
	// 如果是内联的page的话 直接使用
	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	// 读取page info 并缓存下来
	n.read(p)
	b.nodes[pgid] = n
	// 更新事务状态
	b.tx.stats.NodeCount++

	return n
}

// 释放当前 Bucket 对应的 page 到 freelist
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

// TODO 这个函数的应用场景？
// dereference removes all references to the old mmap.
func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}
}

// Cursor相关函数 -- pageNode 要么返回 page 要么返回 node
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// 内联页的话 直接返回其page
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// Check the node cache for non-inline buckets.
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}
	// 确保node中没有再去page中寻找
	return b.tx.page(id), nil
}

// BucketStats records statistics about resources used by a bucket.
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data

	// Bucket statistics
	BucketN           int // total number of buckets including the top bucket
	InlineBucketN     int // total number on inlined buckets
	InlineBucketInuse int // bytes used for inlined buckets (also accounted for in LeafInuse)
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// 重新申请一块内存(默认是对齐的) 并复制v中的内容
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
