package main

import (
	"log"

	ebolt "github.com/impact-eintr/bolt"
)

func main() {
	// 我们的大柜子
	db, err := ebolt.Open("./my.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	// 往db里面插入数据
	err = db.Update(func(tx *ebolt.Tx) error {
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
	err = db.View(func(tx *ebolt.Tx) error {
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
