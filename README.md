## bolt
以下内容来自:
<https://www.bookstack.cn/read/jaydenwen123-boltdb_book/00fe39712cec954e.md>

在用自己的话介绍boltdb之前，我们先看下boltdb官方是如何自我介绍的呢？

> Bolt is a pure Go key/value store inspired by [Howard Chu’s][hyc_symas] [LMDB project][lmdb]. The goal of the project is to provide a simple, fast, and reliable database for projects that don’t require a full database server such as Postgres or MySQL.
> Since Bolt is meant to be used as such a low-level piece of functionality, simplicity is key. The API will be small and only focus on getting values and setting values. That’s it.

看完了官方的介绍，接下来让我用一句话对boltdb进行介绍：

boltdb是一个纯go编写的支持事务的文件型单机kv数据库。

下面对上述几个核心的关键词进行一一补充。

> 纯go：

意味着该项目只由golang语言开发，不涉及其他语言的调用。因为大部分的数据库基本上都是由c或者c++开发的，boltdb是一款难得的golang编写的数据库。

> 支持事务：

boltdb数据库支持两类事务：读写事务、只读事务。这一点就和其他kv数据库有很大区别。

> 文件型： 

boltdb所有的数据都是存储在磁盘上的，所以它属于文件型数据库。这里补充一下个人的理解，在某种维度来看，boltdb很像一个简陋版的innodb存储引擎。底层数据都存储在文件上，同时数据都涉及数据在内存和磁盘的转换。但不同的是，innodb在事务上的支持比较强大。

> 单机：

boltdb不是分布式数据库，它是一款单机版的数据库。个人认为比较适合的场景是，用来做wal日志或者读多写少的存储场景。

> kv数据库： 
 
boltdb不是sql类型的关系型数据库，它和其他的kv组件类似，对外暴露的是kv的接口，不过boltdb支持的数据类型key和value都是[]byte。

其实boltdb的用法很简单，从其项目github的文档里面就可以看得出来。它本身的定位是key/value(后面简称为kv)存储的嵌入式数据库，因此那提到kv我们自然而然能想到的最常用的操作，就是set(k,v)和get(k)了。确实如此boltdb也就是这么简单。

不过在详细介绍boltdb使用之前，我们先以日常生活中的一些场景来作为切入点，引入一些在boltdb中抽象出来的专属名词(DB、Bucket、Cursor、k/v等)，下面将进入正文，前面提到boltdb的使用确实很简单，就是set和get。但它还在此基础上还做了一些额外封装。下面通过现实生活对比来介绍这些概念。

boltdb本质就是存放数据的，那这和现实生活中的柜子就有点类似了，如果我们把boltdb看做是一个存放东西的柜子的话，它里面可以存放各种各样的东西，确实是的，但是我们想一想，所有东西都放在一起会不会有什么问题呢？

咦，如果我们把钢笔、铅笔、外套、毛衣、短袖、餐具这些都放在一个柜子里的话，会有啥问题呢？这对于哪些特别喜欢收拾屋子，东西归类放置的人而言，简直就是一个不可容忍的事情，因为所有的东西都存放在一起，当东西多了以后就会显得杂乱无章。

在生活中我们都有分类、归类的习惯，例如对功能类似的东西(钢笔、铅笔、圆珠笔等)放一起，或者同类型的东西(短袖、长袖等)放一起。把前面的柜子通过隔板来隔开，分为几个小的小柜子，第一个柜子可以放置衣服，第二个柜子可以放置书籍和笔等。当然了，这是很久以前的做法了，现在买的柜子，厂家都已经将其内部通过不同的存放东西的规格做好了分隔。大家也就不用为这些琐事操心了。既然这样，那把分类、归类这个概念往计算机中迁移过来，尤其是对于存放数据的数据库boltdb中，它也需要有分类、归类的思想，因为归根到底，它也是由人创造出来的嘛。

好了到这儿，我们引入我们的三大名词了“DB”、“Bucket”、“k/v”。

DB： 对应我们上面的柜子。

Bucket： 对应我们将柜子分隔后的小柜子或者抽屉了。

k/v： 对应我们放在抽屉里的每一件东西。为了方便我们后面使用的时候便捷，我们需要给每个东西都打上一个标记，这个标记是可以区分每件东西的，例如k可以是该物品的颜色、或者价格、或者购买日期等，v就对应具体的东西啦。这样当我们后面想用的时候，就很容易找到。尤其是女同胞们的衣服和包包，哈哈

再此我们就可以得到一个大概的层次结构，一个柜子(DB)里面可以有多个小柜子(Bucket)，每个小柜子里面存放的就是每个东西(k/v)啦。

那我们想一下，我们周末买了一件新衣服，回到家，我们要把衣服放在柜子里，那这时候需要怎么操作呢？

很简单啦，下面看看我们平常怎么做的。

第一步：如果家里没有柜子，那就得先买一个柜子；

第二步：在柜子里找找之前有没有放置衣服的小柜子，没有的话，那就分一块出来，总不能把新衣服和钢笔放在一块吧。

第三步：有了放衣服的柜子，那就里面找找，如果之前都没衣服，直接把衣服打上标签，然后丢进去就ok啦；如果之前有衣服，那我们就需要考虑要怎么放了，随便放还是按照一定的规则来放。这里我猜大部分人还是会和我一样吧。喜欢按照一定的规则放，比如按照衣服的新旧来摆放，或者按照衣服的颜色来摆放，或者按照季节来摆放，或者按照价格来摆放。哈哈

我们在多想一下，周一早上起来我们要找一件衣服穿着去上班，那这时候我们又该怎么操作呢？

第一步：去找家里存放东西的柜子，家里没柜子，那就连衣服都没了，尴尬…。所以我们肯定是有柜子的，对不对

第二步：找到柜子了，然后再去找放置衣服的小柜子，因为衣服在小柜子存放着。

第三步：找到衣服的柜子了，那就从里面找一件衣服了，找哪件呢！最新买的？最喜欢的？天气下雨了，穿厚一点的？天气升温了，穿薄一点的？今天没准可能要约会，穿最有气质的？…..

那这时候根据不同场景来确定了规则，明确了我们要找的衣服的标签，找起来就会很快了。我们一下子就能定位到要穿的衣服了。嗯哼，这就是排序、索引的威力了

如果之前放置的衣服没有按照这些规则来摆放。那这时候就很悲剧了，就得挨个挨个找，然后自己选了。哈哈，有点全表扫描的味道了

啰里啰嗦扯了一大堆，就是为了给大家科普清楚，一些boltdb中比较重要的概念，让大家对比理解。降低理解难度。下面开始介绍boltdb是如何简单使用的。

``` go

import "bolt"
func main(){
    // 我们的大柜子
    db, err := bolt.Open("./my.db", 0600, nil)
    if err != nil {
        panic(err)
    }
    defer db.Close()
    // 往db里面插入数据
    err = db.Update(func(tx *bolt.Tx) error {
       //我们的小柜子
        bucket, err := tx.CreateBucketIfNotExists([]byte("user"))
        if err != nil {
            log.Fatalf("CreateBucketIfNotExists err:%s", err.Error())
            return err
        }
        //放入东西
        if err = bucket.Put([]byte("hello"), []byte("world")); err != nil {
            log.Fatalf("bucket Put err:%s", err.Error())
            return err
        }
        return nil
    })
    if err != nil {
        log.Fatalf("db.Update err:%s", err.Error())
    }
    // 从db里面读取数据
    err = db.View(func(tx *bolt.Tx) error {
        //找到柜子
        bucket := tx.Bucket([]byte("user"))
        //找东西
        val := bucket.Get([]byte("hello"))
        log.Printf("the get val:%s", val)
        val = bucket.Get([]byte("hello2"))
        log.Printf("the get val2:%s", val)
        return nil
    })
    if err != nil {
        log.Fatalf("db.View err:%s", err.Error())
    }
}
```


### 组织结构

![img](https://static.sitestack.cn/projects/jaydenwen123-boltdb_book/f14d43644ae24228e23d62b62f80a7a3.png)

### 特点
1. mmap

在boltdb中所有的数据都是以page页为单位组织的，那这时候通常我们的理解是，当通过索引定位到具体存储数据在某一页时，然后就先在页缓存中找，如果页没有缓存，则打开数据库文件中开始读取那一页的数据就好了。 但这样的话性能会极低。boltdb中是通过mmap内存映射技术来解决这个问题。当数据库初始化时，就会进行内存映射，将文件中的数据映射到内存中的一段连续空间，后续再读取某一页的数据时，直接在内存中读取。性能大幅度提升。

2. b+树

在boltdb中，索引和数据时按照b+树来组织的。其中一个bucket对象对应一颗b+树，叶子节点存储具体的数据，非叶子节点只存储具体的索引信息，很类似mysql innodb中的主键索引结构。同时值得注意的是所有的bucket也构成了一颗树。但该树不是b+树。

3. 嵌套bucket

前面说到，在boltdb中，一个bucket对象是一颗b+树，它上面存储一批kv键值对。但同时它还有一个特性，一个bucket下面还可以有嵌套的subbucket。subbucket中还可以有subbucket。这个特性也很重要。


## 核心数据结构

从一开始，boltdb的定位就是一款文件数据库，顾名思义它的数据都是存储在磁盘文件上的，目前我们大部分场景使用的磁盘还是机械磁盘。而我们又知道数据落磁盘其实是一个比较慢的操作(此处的快慢是和操作内存想对比而言)。所以怎么样在这种硬件条件无法改变的情况下，如何提升性能就成了一个恒定不变的话题。而提升性能就不得不提到它的数据组织方式了。所以这部分我们主要来分析boltdb的核心数据结构。

我们都知道，操作磁盘之所以慢，是因为对磁盘的读写耗时主要包括：寻道时间+旋转时间+传输时间。而这儿的大头主要是在寻道时间上，因为寻道是需要移动磁头到对应的磁道上，通过马达驱动磁臂移动是一种机械运动，比较耗时。我们往往对磁盘的操作都是随机读写，简而言之，随机读写的话，需要频繁移动磁头到对应的磁道。这种方式性能比较低。还有一种和它对应的方式：顺序读写。顺序读写的性能要比随机读写高很多。

因此，所谓的提升性能，**无非就是尽可能的减少磁盘的随机读写，更大程度采用顺序读写的方式。**这是主要矛盾，不管是mysql的innodb还是boltdb他们都是围绕这个核心来展开的。**如何将用户写进来在内存中的数据尽可能采用顺序写的方式放进磁盘，同时在用户读时，将磁盘中保存的数据以尽可能少的IO调用次数加载到内存中，进而返回用户。**这里面就涉及到具体的数据在磁盘、内存中的组织结构以及相互转换了。下面我们就对这一块进行详细的分析

这里面主要包含几块内容：一个是它在磁盘上的数据组织结构page、一个是它在内存中的数据组织结构node、还有一个是page和node之间的相互转换关系。

这里先给大家直观的科普一点：

set操作： 本质上对应的是 set->node->page->file的过程

get操作： 本质上对应的是 file->page->node->get的过程

#### page

``` go
type pgid int64

// 磁盘数据结构
type page struct {
	// 页id 8byte
	id pgid
	// flags 页类型 可以是分支 叶子节点 叶子节点 空闲列表 2byte
	flags uint16
	// 个数 2byte 统计叶子节点 非叶子节点 空闲列表的个数
	cout uint16
	// 4byte 数据是否有溢出 主要用于空闲列表
	overflow uint32
	// 真实的数据
	ptr uintptr
}

```

![img](https://static.sitestack.cn/projects/jaydenwen123-boltdb_book/47b966145cf27bff53b7d35acbe05554.png)

在boltdb中，它把页划分为四类：

|page页类型|类型定义|类型值|用途|
|:-:|:-:|:-:|:-:|
|分支节点页|branchPageFlag|0x01|存储索引信息(页号、元素key值)|
|叶子节点页|leafPageFlag|0x02|存储数据信息(页号、插入的key值、插入的value值)|
|元数据页|metaPageFlag|0x03|存储数据库的元信息，例如空闲列表页id、放置桶的根页等|
|空闲列表页|freelistPageFlag|0x04|存储哪些页是空闲页，可以用来后续分配空间时，优先考虑分配|


