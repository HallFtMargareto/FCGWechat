package windows

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/sjzar/chatlog/internal/errors"
	"github.com/sjzar/chatlog/internal/wechat/decrypt/common"

	"golang.org/x/crypto/pbkdf2"
)

// V4 版本特定常量
const (
	V4IterCount    = 256000
	HmacSHA512Size = 64
)

// V4Decryptor 实现Windows V4版本的解密器
type V4Decryptor struct {
	// V4 特定参数
	iterCount int
	hmacSize  int
	hashFunc  func() hash.Hash
	reserve   int
	pageSize  int
	version   string
}

// NewV4Decryptor 创建Windows V4解密器
func NewV4Decryptor() *V4Decryptor {
	hashFunc := sha512.New
	hmacSize := HmacSHA512Size
	reserve := common.IVSize + hmacSize
	if reserve%common.AESBlockSize != 0 {
		reserve = ((reserve / common.AESBlockSize) + 1) * common.AESBlockSize
	}

	return &V4Decryptor{
		iterCount: V4IterCount,
		hmacSize:  hmacSize,
		hashFunc:  hashFunc,
		reserve:   reserve,
		pageSize:  PageSize,
		version:   "Windows v4",
	}
}

// deriveKeys 派生加密密钥和MAC密钥
func (d *V4Decryptor) deriveKeys(key []byte, salt []byte) ([]byte, []byte) {
	// 生成加密密钥
	encKey := pbkdf2.Key(key, salt, d.iterCount, common.KeySize, d.hashFunc)

	// 生成MAC密钥
	macSalt := common.XorBytes(salt, 0x3a)
	macKey := pbkdf2.Key(encKey, macSalt, 2, common.KeySize, d.hashFunc)

	return encKey, macKey
}

// Validate 验证密钥是否有效
func (d *V4Decryptor) Validate(page1 []byte, key []byte) bool {
	if len(page1) < d.pageSize || len(key) != common.KeySize {
		return false
	}

	salt := page1[:common.SaltSize]
	return common.ValidateKey(page1, key, salt, d.hashFunc, d.hmacSize, d.reserve, d.pageSize, d.deriveKeys)
}

// Decrypt 解密数据库
func (d *V4Decryptor) Decrypt(ctx context.Context, dbfile string, hexKey string, output io.Writer) error {
	// 解码密钥
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return errors.DecodeKeyFailed(err)
	}

	// 打开数据库文件并读取基本信息
	dbInfo, err := common.OpenDBFile(dbfile, d.pageSize)
	if err != nil {
		return err
	}

	// 验证密钥
	if !d.Validate(dbInfo.FirstPage, key) {
		return errors.ErrDecryptIncorrectKey
	}

	// 计算密钥
	encKey, macKey := d.deriveKeys(key, dbInfo.Salt)

	// 读取整个文件到内存（为了并行处理）
	fileData, err := os.ReadFile(dbfile)
	if err != nil {
		return errors.ReadFileFailed(dbfile, err)
	}

	// 写入SQLite头
	_, err = output.Write([]byte(common.SQLiteHeader))
	if err != nil {
		return errors.WriteOutputFailed(err)
	}

	// 并行解密
	numWorkers := runtime.NumCPU()
	results := make([]struct {
		pageNum int64
		data    []byte
		err     error
	}, dbInfo.TotalPages)

	var wg sync.WaitGroup
	pageChan := make(chan int64, numWorkers)
	var hasError atomic.Bool
	var firstErr atomic.Value // 存储第一个错误

	// 启动 worker 协程
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					// 捕获 panic 并记录
					fmt.Fprintf(os.Stderr, "Decrypt worker %d panic: %v\nStack trace:\n%s\n", workerID, r, debug.Stack())
					firstErr.CompareAndSwap(nil, fmt.Errorf("decrypt worker panic: %v", r))
					hasError.Store(true)
				}
			}()

			for pageNum := range pageChan {
				// 如果已有错误，快速返回
				if hasError.Load() {
					return
				}

				select {
				case <-ctx.Done():
					return
				default:
				}

				start := pageNum * int64(d.pageSize)
				end := start + int64(d.pageSize)
				if end > int64(len(fileData)) {
					end = int64(len(fileData))
				}

				pageBuf := make([]byte, d.pageSize)
				// 安全地复制数据，避免越界
				copyLength := copy(pageBuf, fileData[start:end])
				if copyLength < int(end-start) && copyLength < d.pageSize {
					// 如果复制的长度不够，填充剩余部分为 0
					for j := copyLength; j < d.pageSize; j++ {
						pageBuf[j] = 0
					}
				}

				// 检查页面是否全为零
				allZeros := true
				for _, b := range pageBuf {
					if b != 0 {
						allZeros = false
						break
					}
				}

				if allZeros {
					results[pageNum] = struct {
						pageNum int64
						data    []byte
						err     error
					}{pageNum, pageBuf, nil}
					continue
				}

				// 解密页面
				decryptedData, err := common.DecryptPage(pageBuf, encKey, macKey, pageNum, d.hashFunc, d.hmacSize, d.reserve, d.pageSize)
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					hasError.Store(true)
					results[pageNum] = struct {
						pageNum int64
						data    []byte
						err     error
					}{pageNum, nil, err}
					return
				}

				results[pageNum] = struct {
					pageNum int64
					data    []byte
					err     error
				}{pageNum, decryptedData, nil}
			}
		}(i)
	}

	// 发送任务
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "Task sender panic: %v\nStack trace:\n%s\n", r, debug.Stack())
				firstErr.CompareAndSwap(nil, fmt.Errorf("task sender panic: %v", r))
				hasError.Store(true)
			}
		}()

		for curPage := int64(0); curPage < dbInfo.TotalPages; curPage++ {
			if hasError.Load() {
				break
			}
			pageChan <- curPage
		}
		close(pageChan)
	}()

	// 等待完成
	wg.Wait()

	// 检查是否有错误
	if errVal := firstErr.Load(); errVal != nil {
		return errVal.(error)
	}

	// 检查上下文是否被取消
	select {
	case <-ctx.Done():
		return errors.ErrDecryptOperationCanceled
	default:
	}

	// 按顺序写入结果
	for curPage := int64(0); curPage < dbInfo.TotalPages; curPage++ {
		result := results[curPage]
		if result.err != nil {
			return result.err
		}
		if result.data == nil {
			return fmt.Errorf("page %d decryption result is nil", curPage)
		}
		_, err = output.Write(result.data)
		if err != nil {
			return errors.WriteOutputFailed(err)
		}
	}

	return nil
}

// GetPageSize 返回页面大小
func (d *V4Decryptor) GetPageSize() int {
	return d.pageSize
}

// GetReserve 返回保留字节数
func (d *V4Decryptor) GetReserve() int {
	return d.reserve
}

// GetHMACSize 返回HMAC大小
func (d *V4Decryptor) GetHMACSize() int {
	return d.hmacSize
}

// GetVersion 返回解密器版本
func (d *V4Decryptor) GetVersion() string {
	return d.version
}

// GetIterCount 返回迭代次数（Windows特有）
func (d *V4Decryptor) GetIterCount() int {
	return d.iterCount
}
