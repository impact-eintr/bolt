package bolt

import (
	"bytes"
	"fmt"
	"sort"
)

type Cursor struct {
	bucket *Bucket   // 使用该句柄来进行 node 的加载
	stack  []elemRef // 保留路径，方便回溯 []{node1, page1, page2, node2, node3, page3, node4, ...}
}

// 返回持有当前游标的 Bucket对象
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// Cursor 的开放接口
// Bucket() 返回持有当前 Cursor 的Bucket对象
// First()  返回持有当前 Cursor 的 Bucket 的第一个 K/V
// Last()   返回持有当前 Cursor 的 Bucket 的最后一个 K/V
// Prev()   返回持有当前 Cursor 所在位置的 Bucket 的前一个 K/V
// Next()   返回持有当前 Cursor 所在位置的 Bucket 的下一个 K/V
// Seek()   返回持有当前 Cursor 的 Bucket 的某个指定 key 的 K/V
// Delete() 删除持有当前 Cursor 的 Bucket 的某个指定 key 的 K/V

// 定位到并返回该 bucket 第一个 KV
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx close")
	// 清空stack
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	// 一直找到第一个叶子节点，此处再添加stack时，一直让index=0即可
	ref := elemRef{page: p, node: n}
	c.stack = append(c.stack, ref)

	c.first()

	// 当前页时空的话，找下一个
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	// 检测是否为一个子桶
	if (flags * uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 定位到并返回该 bucket 最后一个 KV
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	// 设置其index为当前页元素的最后一个
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)

	c.last()

	k, v, flags := c.keyValue()
	// 检测是否为一个子桶
	if (flags * uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 定位到并返回 当前Cursor所在位置的 下一个 KV
// 再此我们从当前位置查找前一个或者下一个时，需要注意一个问题:
// 如果当前节点中元素已经完了，那么此时需要回退到遍历路径的上一个节点
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// 定位到并返回 当前Cursor所在位置的 上一个 KV
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	// Attempt to move back one element until we're successful.
	// Move up the stack as we hit the beginning of each page in our stack.
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 { // elem.index 是当前 elem 在inodes中的位置 > 0 说明不是第一个 不需要向上找了
			// 往前移动一格
			elem.index--
			break
		}
		c.stack = c.stack[:i] // 清理路径 作为终止条件
	}
	// If we've hit the end then return nil.
	if len(c.stack) == 0 {
		return nil, nil
	}
	// Move down the stack to find the last element of the last leaf under this branch.
	// 如果当前节点是叶子节点的话，则直接退出了，啥都不做。
	c.last()
	// 否则的话移动到新页的最后一个节点
	//      4 7
	// 2 3 4   5 6 7
	// 5 的上一个是 4 是[4]这一侧的last
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Seek 搜寻
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seek(seek)

	// 下面这一段逻辑是必须的，因为在seek()方法中，如果ref.index>ref.count()的话，就直接返回nil,nil,0了
	// 这里需要返回下一个
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}
	if k == nil {
		return nil, nil
		// 子桶的话
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Delete 从 Bucket 中删除当前 Cursor 下 K/V
// 如果当前键/值是存储桶或事务不可写，则删除失败
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}
	key, _, flags := c.keyValue()
	// Return an error if current value is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	// 从node中移除，本质上将inode数组进行移动
	c.node().del(key)
	return nil
}

// 借助 search，查询 key 对应的 value
// 如果 key 不存在，则返回待插入位置的 kv 对：
//   1. ref.index < ref.node.count() 时，则返回第一个比给定 key 大的 kv
//   2. ref.index == ref.node.count() 时，则返回 nil
// 上层 Seek 需要处理第二种情况。
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0] // 清空stack
	// 开始根据seek的key值搜索root
	c.search(seek, c.bucket.root)

	// 执行完搜索后 stack中保存了所遍历的路径
	ref := &c.stack[len(c.stack)-1] // 最后一个节点是叶子

	// If the cursor is pointing to the end of page/node then return nil.
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	// 获取值
	return c.keyValue()
}

// 移动 cursor 到以栈顶元素为根的子树中最左边的叶子节点
// 找到最后一个非叶子节点的第一个叶子节点。index=0的节点
func (c *Cursor) first() {
	for {
		// Exit when we hit a leaf page.
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}
		// Keep adding pages pointing to the first element to the stack.
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// 移动 cursor 到以栈顶元素为根的子树中最右边的叶子节点
// 移动到栈中最后一个节点的最后一个叶子节点
func (c *Cursor) last() {
	for {
		// Exit when we hit a leaf page.
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}
		// Keep adding pages pointing to the last element in the stack.
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// 移动 cursor 到下一个叶子元素
//   1. 如果当前叶子节点后面还有元素，则直接返回
//   2. 否则需要借助保存的路径找到下一个非空叶子节点
//   3. 如果当前已经指向最后一个叶子节点的最后一个元素，则返回 nil
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 { // 说明当前的栈中元素并不是本层的最后一个元素
				// 元素还有时，往后移动一个
				elem.index++
				break
			}
		}
		// 最后的结果elem.index++
		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		// 所有页都遍历完了 没有下一个了
		if i == -1 {
			return nil, nil, 0
		}

		// 剩余的节点里面找，跳过原先遍历过的节点
		c.stack = c.stack[:i+1] // i+1才包含i

		// 如果是叶子节点，first()啥都不做，直接退出。返回elem.index+1的数据
		c.first()
		// 非叶子节点的话，需要移动到stack中最后一个路径的第一个元素
		//      4 7
		// 2 3 4   5 6 7
		// 4 的下一个是 5 是[7]这一侧的first
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}
		return c.keyValue()
	}
}

// 尾递归，查询 key 所在的 node，并且在 cursor 中记下路径
func (c *Cursor) search(key []byte, pgid pgid) {
	// root, 3
	// 层层找page bucket->tx->db->dataref
	p, n := c.bucket.pageNode(pgid) // 从根节点开始 返回 root 的 page / node
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("incalid page type: %d: %x", p.id, p.flags))
	}

	e := elemRef{page: p, node: n}
	// 记录遍历过的路径
	c.stack = append(c.stack, e)
	// 如果是叶子节点(search递归结束)
	if e.isLeaf() {
		c.nsearch(key) // 如果找到的话 c.stack[-1].index会被设置为正确元素的位置索引
		return
	}

	// 如果不是叶子而是分支
	if n != nil {
		// 先搜索node，因为node是加载到内存中的
		c.searchNode(key, n) // 这个函数中有递归调用 search
		return
	}
	// 内存中没有再去page中找
	c.searchPage(key, p)
}

// node中搜索 其中递归调用了search
func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	// 二分搜索
	index := sort.Search(len(n.inodes), func(i int) bool {
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true // 索引和要找的值正好相等
		}
		return ret != -1
	})
	// 找6
	//             8 12
	//           /      \
	//     |3 8|          |10 12|
	//     /   \           /    \
	//|1 2 3||4 5 6 7 8| |9 10| |11 12|
	//  8->8->(二分找到6)
	if !exact && index > 0 { // 没找到 但已经到了目标边界 进去接着找
		index--
	}
	c.stack[len(c.stack)-1].index = index // 将相关数据入栈存放
	c.search(key, n.inodes[index].pgid)
}

// page中搜索
func (c *Cursor) searchPage(key []byte, p *page) {
	// 老样子 二分查找
	inodes := p.branchPageElements()
	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true // 找到了
		}
		return ret != -1
	})

	if !exact && index > 0 { // 没找到 但已经到了目标边界 进去接着找
		index--
	}
	c.stack[len(c.stack)-1].index = index
	// 继续递归
	c.search(key, inodes[index].pgid)
}

// 搜索叶子节点(一个数组) 中是否有 key
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node
	// 先搜索node
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1 // 按照字典序 检测 n.inodes[i].key >= key
		})
		e.index = index
		return
	}
	// 然后搜索page
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// keyValue returns the key and value of the current leaf element(叶子).
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	// 最后一个节点是叶子节点
	ref := &c.stack[len(c.stack)-1] // stack是一条路径 node->node->node->leaf
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// 首先从内存中取
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}
	// 然后从文件page中取
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

// 返回当前 Cursor 当前所在的节点
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node a zero-length cursor stack")
	// If the top of the stack is a leaf node then just return it
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// 从根开始向下遍历层次结构
	var n = c.stack[0].node
	if n == nil {
		// 没东西的话加载一个
		n = c.bucket.node(c.stack[0].page.id, nil)
	}

	// 遍历 并 检验
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(ref.index)
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef 表示对给定 page/node 上元素的引用。
type elemRef struct {
	node  *node
	page  *page
	index int // 表示路径经过该节点时在 inodes 中的位置
}

func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
