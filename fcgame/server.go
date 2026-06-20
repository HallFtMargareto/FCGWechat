package fcgame

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
)

type ServerInfo struct {
	Domain   string `json:"domain"`
	Version  int    `json:"version"`
	Download string `json:"download"`
}

type ServerInfoResponse struct {
	Code int        `json:"code"`
	Data ServerInfo `json:"data"`
	Msg  string     `json:"msg"`
}

var (
	serverInfoCache atomic.Value
	serverInfoAt    atomic.Int64
)

func init() {
	serverInfoCache.Store(ServerInfo{
		Domain:   "E5BwQ/fCRGpKLwnw4DT+sMmk+N3Ngh1YhrGirD/RGpG5M7pLixUpzyF14d84pvioXq/MkemtebEBx+DeQcU8IX0mwFpP",
		Version:  Version,
		Download: "",
	})
	serverInfoAt.Store(time.Now().Unix())
}

func GetServerInfo() (ServerInfo, bool) {
	v := serverInfoCache.Load()
	if v == nil {
		return ServerInfo{}, false
	}
	info, ok := v.(ServerInfo)
	return info, ok
}

func RefreshServerInfo() {
	ctx := context.Background()
	u := buildServerInfoURL()
	if u == "" {
		return
	}

	info, err := FetchServerInfo(ctx, u)
	if err != nil {
		if Logger != nil {
			Logger.Warn("刷新配置失败", zap.Error(err))
		}
		return
	}

	if info.Version == 0 {
		info.Version = Version
	}

	domains := make([]string, 0)
	plainText, err := decryptAESGCM(info.Domain, getClientSecretKey())
	if err != nil {
		if Logger != nil {
			Logger.Warn("解密配置失败", zap.Error(err))
		}
		return
	}
	err = json.Unmarshal([]byte(plainText), &domains)
	if err != nil {
		if Logger != nil {
			Logger.Warn("解析配置失败", zap.Error(err))
		}
		return
	}

	rz := false
	for _, v := range domains {
		if v == strings.TrimSpace(WSServer) {
			rz = true
			break
		}
	}
	DomainRZ = rz

	serverInfoCache.Store(info)
	serverInfoAt.Store(time.Now().Unix())

	if info.Version > Version {
		msg := "当前客户端版本过旧，请更新。下载地址：" + strings.TrimSpace(info.Download)
		fmt.Println(msg)
		if Logger != nil {
			Logger.Warn(msg, zap.Int("local_version", Version), zap.Int("server_version", info.Version))
		}
	}
}

func buildServerInfoURL() string {
	scheme := "http"
	if strings.EqualFold(WSScheme, "wss") {
		scheme = "https"
	}
	host := strings.TrimSpace(WSServer)
	if host == "" {
		return ""
	}
	port := strings.TrimSpace(WSPort)
	if port != "" && port != "80" {
		host = host + ":" + port
	}
	u := url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   "/test/client",
	}
	return u.String()
}

func FetchServerInfo(ctx context.Context, url string) (ServerInfo, error) {
	if strings.TrimSpace(url) == "" {
		return ServerInfo{}, errors.New("empty url")
	}

	client := resty.New().SetTimeout(10 * time.Second)
	resp, err := client.R().SetContext(ctx).Get(url)
	if err != nil {
		return ServerInfo{}, err
	}

	if resp.StatusCode() < 200 || resp.StatusCode() >= 300 {
		return ServerInfo{}, fmt.Errorf("unexpected status: %s", resp.Status())
	}

	var out ServerInfoResponse
	if err := json.Unmarshal(resp.Body(), &out); err != nil {
		return ServerInfo{}, err
	}
	if out.Code != 0 {
		if out.Msg != "" {
			return ServerInfo{}, fmt.Errorf("serverinfo error: code=%d msg=%s", out.Code, out.Msg)
		}
		return ServerInfo{}, fmt.Errorf("serverinfo error: code=%d", out.Code)
	}
	if strings.TrimSpace(out.Data.Domain) == "" {
		return ServerInfo{}, errors.New("serverinfo missing domain")
	}
	return out.Data, nil
}

func decryptAESGCM(cipherTextBase64 string, secret string) (string, error) {
	cipherData, err := base64.StdEncoding.DecodeString(cipherTextBase64)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256([]byte(secret))
	block, err := aes.NewCipher(hash[:])
	if err != nil {
		return "", err
	}
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonceSize := aesGCM.NonceSize()
	if len(cipherData) <= nonceSize {
		return "", errors.New("cipher text is too short")
	}
	nonce := cipherData[:nonceSize]
	cipherText := cipherData[nonceSize:]
	plainText, err := aesGCM.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return "", err
	}
	return string(plainText), nil
}

func getClientSecretKey() string {
	return "fcg-client-config-key"
}
