package bolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// 在内存中，分支节点 和 叶子节点 都是通过node来表示
// 只不过的区别是通过其node中的isLeaf这个字段来区分
// 1. 对叶子节点而言，其没有children这个信息。同时也没有key信息。
//    isLeaf字段为true，其上存储的key、value都保存在inodes中
// 2. 对于分支节点而言，其具有key信息，同时children也不一定为空。
//    isLeaf字段为false，同时该节点上的数据保存在inode中。
type node struct {
	bucket     *Bucket // 关联一个桶 桶由各种 node 组成
	isLeaf     bool    // 标志是否为叶子
	unbalanced bool    // 值为true的话，需要考虑页合并 是否不平衡
	spilled    bool    // 值为false的话，需要考虑页分裂
	key        []byte  // 对于分支节点而言 保留的是 最小的key 对于叶子节点 该值为空
	pgid       pgid    // 分支节点关联的页id
	parent     *node   // 该节点的parent
	children   nodes   // 分支节点的孩子节点 叶子节点该值为空
	inodes     inodes  // 该节点上保存的索引数据 是个数组
}

// 寻根
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value) // 头数据 + KSize + VSize
	}
	return sz
}

func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value) // 头数据 + KSize + VSize
		if sz >= v {
			return false
		}
	}
	return true
}

func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// childAt return the child node at a given index.
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n)
}

// childIndex returns the index of a given child node.
func (n *node) childIndex(child *node) int {
	return sort.Search(len(n.inodes), func(i int) bool {
		return bytes.Compare(n.inodes[i].key, child.key) != -1
	})
}

func (n *node) numChildren() int {
	return len(n.inodes)
}

// 返回下一个兄弟节点
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// 返回上一个兄弟节点
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.
// 如果flags值为 1 表示子桶节点，否则为 0 标志普通叶子节点
// 如果put的是一个key、value的话，不需要指定pgid。
// 如果put的一个树枝节点，则需要指定pgid，不需要指定value
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid(%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// node.key node.inodes[0].key

	// 二分查找到待插入位置
	index := sort.Search(len(n.inodes), func(i int) bool {
		return bytes.Compare(n.inodes[i].key, oldKey) != -1
	}) // 获取第一个小于给定 key 的元素下标

	exact := (len(n.inodes) > 0 && index < len(n.inodes) &&
		bytes.Equal(n.inodes[index].key, oldKey)) // 索引到的位置上已有的key 与准备put的key相同
	// 如果是覆盖写则直接覆盖
	// 否则就要新增一个元素，并整体右移，腾出插入位置
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// get(k)
// 在node中，没有get(k)的方法，其本质是在Cursor中就返回了get的数据。
// -> Cursor.keyValue()

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })
	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}
	// Delete inode from the node.
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)
	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// v->file->page->node-> Get(k)
// 根据page来初始化node
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.inodes = make(inodes, int(p.count))

	// 一个page中包含有多个node的数据
	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i] // 拿到每个node的句柄

		if n.isLeaf {
			// 获取第i个叶子节点
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			// 获取分支节点
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	if len(n.inodes) > 0 {
		// 保存第一个元素的值
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// Set(k, v) ->node->page->file
// 这里是 node -> page
func (n *node) write(p *page) {
	// Initialize page
	// 判断是否是叶子节点还是非叶子节点
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	// 这里的叶子节点不应该溢出 因为溢出时 会发生分裂
	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d(pgid = %d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))
	// Stop here if there are no items to write
	if p.count == 0 {
		return
	}

	// b指向的指针为跳过所有item头部的位置
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// 写入叶子节点头部数据
		if n.isLeaf {
			elem := p.leafPageElement((uint16(i)))
			// uintptr(unsafe.Pointer(&b[0])) 准备写入位置的首地址
			// uintptr(unsafe.Pointer(elem)) 当前元素的头信息首地址
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.pgid = item.pgid
			elem.ksize = uint32(len(item.key))
			_assert(elem.pgid != p.id, "write: circular dependency pccurred")
		}

		klen, vlen := len(item.key), len(item.value)
		if len(b) < klen+vlen {
			// 重新申请内存 来存放KV
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:]
		}

		// write data for the element to the end of the page
		// 写一个key b往后走一个keylen
		copy(b[0:], item.key)
		b = b[klen:]

		// 写一个value b往后走一个valuelen
		copy(b[0:], item.value)
		b = b[vlen:]
	}
}

// ============ node节点的分裂和合并 =============
func (n *node) split(pageSize int) []*node {
	var nodes []*node
	node := n // 操作指针
	for {
		a, b := node.splitTwo(pageSize) // 不断将当前node分裂
		nodes = append(nodes, a)

		if b == nil {
			break
		}
		node = b
	}
	return nodes
}

func (n *node) splitTwo(pageSize int) (*node, *node) {
	// 太小的话就不拆分了
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// 检查FillPercent 防止溢出
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	// threshold临界值
	threshold := int(float64(pageSize) * fillPercent)

	splitIndex, _ := n.splitIndex(threshold) // 根据临界值找到的分割下标

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// 拆分出一个新节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next) // 将新节点加入当前节点的兄弟节点

	// 拆分数据
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// 更新状态
	n.bucket.tx.stats.Split++

	return n, next
}

// 根据临界值到合适的Index
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize

	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		if i >= minKeysPerPage && sz+elsize > threshold {
			break
		}
		sz += elsize
	}
	return
}

// spill 自顶向下
// 分裂 当一个node中的数据过多时，最简单就是当超过了page的填充度时，
// 就需要将当前的node拆分成两个，也就是底层会将一页数据拆分存放到两页中
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled { // 分裂过了
		return nil
	}
	// 先将子节点排序
	sort.Sort(n.children)
	// 首先spill子节点
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}
	// 我们不再需要child list，因为它仅用于溢出跟踪
	n.children = nil

	// 将当前的node进行拆分成 多个node(一直分到单个node的最小值)
	// 分裂结束后 数据也分裂完毕 状态也已经更新
	var nodes = n.split(tx.db.pageSize)
	for _, node := range nodes {
		// Add node's page to the freelist if it's not new.
		// 释放node占用的page
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}
		// 为节点分配连续的空间 让node重新落盘 p 为 page
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// Write the node
		if p.id >= tx.meta.pgid {
			// 不可能发生
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}

		// 更新 node 的 pgid 并把这个 node 记录到新申请的 page 中
		node.pgid = p.id
		node.write(p)

		// 标志当前 node 已经拆分过了
		node.spilled = true

		// Insert into parent inodes
		// 为了避免在叶子节点最左侧插入一个很小的值时，引起祖先节点的 node.key 的链式更新
		// 而将更新延迟到了最后 B+ 树调整阶段（spill 函数）进行统一处理
		if node.parent != nil { // 如果不是根节点
			var key = node.key // split 后，最左边 node
			if key == nil {    // split 后，非最左边 node
				key = node.inodes[0].key
			}
			// 减过肥的儿子 申请重新加入(spill 过的 node 重新插入 其父亲节点)
			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0) // key
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}
		tx.stats.Spill++
	}
	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	// 如果根节点分裂并创建了一个新根，那么我们也需要溢出它。我们将清除孩子，以确保它不会试图重新溢出
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}
	return nil
}

// 合并 自底向上
// 当删除了一个或者一批对象时，此时可能会导致一页数据的填充度过低
// 此时空间可能会浪费比较多。所以就需要考虑对页之间进行数据合并
// 填充率太低或者没有足够的key时，进行页合并
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false // 标志位置为已经平衡

	// 更新状态
	n.bucket.tx.stats.Rebalance++

	// Ignore if node is above threshold (25%) and has enough keys.
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// root 需要特殊处理一下
	if n.parent == nil {
		// 如果根节点是一个分支并且只有一个节点，则折叠它。2个以上就不必了
		//    [root]
		//    |
		//    [b]
		//    [inode1 inode2 inode3 inode4...]
		//    折叠
		//    [root]
		//    [inode1 inode2 inode3 inode4...]
		if !n.isLeaf && len(n.inodes) == 1 {
			// 提升root的子节点
			child := n.bucket.node(n.inodes[0].pgid, n) // 以n为parent的节点
			// 将root替换
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Remove all child nodes being moved(删除所有正在移动的子节点)
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}
		return
	}

	// 如果当前 node 没有 KV 直接移除(无用节点)
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// 如果 idx == 0，则目标节点是右兄弟节点，否则为左兄弟节点。
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	// 判断当前node是否是parent的第一个孩子节点
	// 是的话，就要找它的下一个兄弟节点，否则的话，就找上一个兄弟节点
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// 合并当前node和target，target合到node
	if useNextSibling {
		// 第二个合并给第一个
		// Reparent all child nodes being moved.
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				// 之前的父亲移除该孩子
				child.parent.removeChild(child)
				// 重新指定父亲节点
				child.parent = n
				// 父亲节点指当前孩子
				child.parent.children = append(child.parent.children, child)
			}
		}
		// Copy over inodes from target and remove target.
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// 上一个合并到当前
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}
		// Copy over inodes to target and remove node.
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	n.parent.rebalance() // 递归进行
}

// 删除某个子节点
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target { // 找到要删除的node
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data.
func (n *node) dereference() {
	if n.key != nil {
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	n.bucket.tx.stats.NodeDeref++
}

// free adds the node's underlying(底层的) page to the freelist
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

type nodes []*node

func (s nodes) Len() int { return len(s) }

func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1
}

type inode struct {
	// 表示是否是子桶叶子节点还是普通叶子节点
	// 如果flags值为 1 表示子桶节点，否则为 0 标志普通叶子节点
	flags uint32
	// 当inode为分支元素时，pgid才有值，为叶子元素时，则没值
	pgid pgid
	key  []byte
	// 当inode为分支元素时，value为空，为叶子元素时，才有值
	value []byte
}

type inodes []inode
