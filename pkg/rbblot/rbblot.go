package rbblot

import (
	"go.etcd.io/bbolt"
)

type RBblotCore struct {
	DB *bbolt.DB
}

func NewBblot(memory int) *RBblotCore {
	db, err := bbolt.Open("fcgame.dat", 0600, &bbolt.Options{
		InitialMmapSize: memory * 1024 * 1024,
	})
	if err != nil {
		panic(err)
	}
	return &RBblotCore{DB: db}
}

// 写数据
func (r *RBblotCore) Store(bucket string, key string, val []byte) {
	r.DB.Update(func(tx *bbolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte(bucket))
		bucket.Put([]byte(key), val)
		return nil
	})
}

// 读取数据
func (r *RBblotCore) Get(bucket string, key string) (val []byte) {
	r.DB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		if bucket == nil {
			return nil
		}
		val = bucket.Get([]byte(key))
		return nil
	})
	return
}

// 批量保存数据
func (r *RBblotCore) BatchStore(bucket string, data map[string][]byte) {
	r.DB.Batch(func(tx *bbolt.Tx) error {
		bucket, _ := tx.CreateBucketIfNotExists([]byte(bucket))
		for key, val := range data {
			bucket.Put([]byte(key), val)
		}
		return nil
	})
}

func (r *RBblotCore) Close() {
	r.DB.Close()
}
