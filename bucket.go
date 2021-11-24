package bolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{})) // bucket是Bucket的Header

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

const DefaultFillPercent = 0.5

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

// 一组key/value的集合 也就是一个b+树
type Bucket struct {
	*bucket
	tx       *Tx
	buckets  map[string]*Bucket
	page     *page
	rootNode *node // 每个b+树的根节点
	nodes    map[pgid]*node
	// 填充率
	FillPercent float64
}

// 存储桶表示存储桶的文件表示
type bucket struct {
	root     pgid
	sequence uint64
}

func newBucket(tx *Tx) Bucket {
	var b = Bucket{
		tx:          tx,
		FillPercent: DefaultFillPercent,
	}
	return b
}

// 既然一个Bucket逻辑上是一颗b+树，那就意味着我们可以对其进行遍历
// 前面提到的set、get操作，无非是要在Bucket上先找到合适的位置，然后再进行操作
// 而“找”这个操作就是交由Cursor(游标)来完成的
// 简而言之对Bucket这颗b+树的遍历工作由Cursor来执行, 一个Bucket对象关联一个Cursor。
func (b *Bucket) Cursor() *Cursor {
	// 更新事务
	b.tx.stats.CursorCount++

	// Allocate and return a cursor
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Cursor相关函数 -- pageNode
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

// 根据pgid创建一个node
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
		// TODO 这样写和直接 append 有没有区别?
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

// =================== Bucket的相关操作 =====================

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

func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writeable {
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

	// 拿到bucket对应的value
	var value = bucket.write()

	// Insert into node
	key = cloneBytes(key)
	// 插入到inode中(叶子)
	// c.node()方法会在内存中建立这颗树 调用n.read(page)
	// c.node() -> node.ChildAt(index int)[这里有一个for] -> bucket.node(pgid, *node) ->
	//   根据pgid创建一个node -内存中没有-> node.read(page) -> 根据page来初始化node
	c.node().put(key, key, value, 0, bucketLeafFlag)

	b.page = nil

	return b.Bucket(key), nil
}

// 内联桶的话 其value中bucketHeaderSize后面的内容为其page的数据
func (b Bucket) write() []byte {
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

func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}

// 获取一个Bucket
// 根据指定的key来获取一个Bucket。如果找不到则返回nil。
func (b *Bucket) Bucket(name []byte) *Bucket {
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
	// 加速缓存的作用
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}
	return child
}

func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0 // 判断是否8对齐

	if unaligned {
		value = cloneBytes(value)
	}

	if b.tx.writeable && !unaligned {
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
	// 将该桶下面的所有桶都删除
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
	// Remove cached copy.
	delete(b.buckets, string(key))
	// Release all bucket pages to freelist.
	child.nodes = nil
	child.rootNode = nil
	child.free()
	// Delete the node if we have a matching key.
	c.node().del(key)
	return nil
}

func (b *Bucket) Writable() bool {
	return b.tx.writable
}

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

// ================= key/value的插入 获取 删除 ====================
// 其实本质上，对key/value的所有操作最终都要表现在底层的node上。因为node节点就是用来存储真实数据的

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
	c.node().put(key, key, value, 0, 0)

	return nil
}

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

// ==================== Bucket的页分裂 页合并 ======================
func (b *Bucket) spill() error {

}

func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}

	for _, child := range b.buckets {
		child.rebalance()
	}
}
