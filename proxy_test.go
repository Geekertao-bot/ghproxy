package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// trackingBody 记录 Close 是否被调用，用于验证响应体被正确关闭。
type trackingBody struct {
	r      io.Reader
	closed bool
}

func (b *trackingBody) Read(p []byte) (int, error) { return b.r.Read(p) }
func (b *trackingBody) Close() error {
	b.closed = true
	return nil
}

// setTestGinMode 设置测试模式并在测试结束后恢复原模式，避免污染全局状态。
func setTestGinMode(t *testing.T) {
	t.Helper()
	old := gin.Mode()
	gin.SetMode(gin.TestMode)
	t.Cleanup(func() { gin.SetMode(old) })
}

func withFakeTransport(t *testing.T, resp *http.Response, body *trackingBody) {
	t.Helper()
	old := httpClient
	httpClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			resp.Request = r
			return resp, nil
		}),
		// 阻止自动跟随 302，以便直接观察到 proxy 对 Location 的处理。
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	t.Cleanup(func() { httpClient = old })
}

func newTestContext(method, target string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, target, nil)
	return c, w
}

// 9. 代理应正确转发响应内容，并关闭上游响应体。
func TestProxyForwardsAndClosesBody(t *testing.T) {
	setTestGinMode(t)
	const payload = "hello ghproxy streaming body"
	body := &trackingBody{r: strings.NewReader(payload)}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Length": []string{strconv.Itoa(len(payload))}},
		Body:       body,
	}
	withFakeTransport(t, resp, body)

	c, w := newTestContext(http.MethodGet, "/https://raw.githubusercontent.com/a/b/main/x.txt")
	proxy(c, "https://raw.githubusercontent.com/a/b/main/x.txt", config.Load())

	if w.Code != http.StatusOK {
		t.Fatalf("状态码应为 200，实际 %d", w.Code)
	}
	if w.Body.String() != payload {
		t.Fatalf("转发内容错误: %q", w.Body.String())
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 超过文件大小限制时应返回 413 并关闭响应体（不读取整个文件）。
func TestProxyRejectsOversize(t *testing.T) {
	setTestGinMode(t)
	oldCfg := config.Load()
	t.Cleanup(func() { config.Store(oldCfg) })
	cfg := &Config{RequestLimit: RequestLimitConfig{LimitSize: 1}} // 1 MB
	config.Store(cfg)

	body := &trackingBody{r: strings.NewReader("tiny")}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Length": []string{strconv.Itoa(2 * 1024 * 1024)}},
		Body:       body,
	}
	withFakeTransport(t, resp, body)

	c, w := newTestContext(http.MethodGet, "/https://raw.githubusercontent.com/a/b/main/big.bin")
	proxy(c, "https://raw.githubusercontent.com/a/b/main/big.bin", cfg)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("状态码应为 413，实际 %d", w.Code)
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 入口链接的 302 应重写为代理地址，而不是内部跟随。
func TestProxyRewritesEntryLocation(t *testing.T) {
	setTestGinMode(t)
	body := &trackingBody{r: strings.NewReader("")}
	resp := &http.Response{
		StatusCode: http.StatusFound,
		Header:     http.Header{"Location": []string{"https://github.com/a/b/releases/download/v1/x.zip"}},
		Body:       body,
	}
	withFakeTransport(t, resp, body)

	c, w := newTestContext(http.MethodGet, "/https://github.com/a/b/releases/download/v1/x.zip")
	proxy(c, "https://github.com/a/b/releases/download/v1/x.zip", config.Load())

	loc := w.Header().Get("Location")
	if loc != "/https://github.com/a/b/releases/download/v1/x.zip" {
		t.Fatalf("Location 重写错误: %q", loc)
	}
}

// 达到限流上限后，handler 直接返回 429，不进入代理流程。
func TestHandlerReturns429WhenLimited(t *testing.T) {
	setTestGinMode(t)
	oldLimiter := limiter
	oldCfg := config.Load()
	t.Cleanup(func() {
		limiter = oldLimiter
		config.Store(oldCfg)
	})

	limiter = newRateLimiter()
	config.Store(&Config{RequestLimit: RequestLimitConfig{LimitRate: 1}})

	// 通过生产入口预先占满该 IP 的唯一额度。
	if !limiter.allow("1.2.3.4", 1) {
		t.Fatal("预填充失败")
	}

	c, w := newTestContext(http.MethodGet, "/https://raw.githubusercontent.com/a/b/main/x.txt")
	c.Request.RemoteAddr = "1.2.3.4:12345"
	handler(c)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("应返回 429，实际 %d", w.Code)
	}
}

// 上游限流响应体里带有本服务的出口 IP（实测 GitHub API 文案：
// "API rate limit exceeded for 198.98.52.152."），不应原样转发给客户端。
func TestProxyHidesUpstreamRateLimitBody(t *testing.T) {
	setTestGinMode(t)
	// 用文档保留地址 TEST-NET-2（198.51.100.0/24）代替真实出口 IP。
	const leaked = `{"message":"API rate limit exceeded for 198.51.100.7. (But here's the good news: ...)","documentation_url":"https://docs.github.com/..."}`
	body := &trackingBody{r: strings.NewReader(leaked)}
	// 用 http.Header 的 Set 构造：Go 会把键规范化为 X-Ratelimit-*（不是
	// 字面上的 X-RateLimit-*），手写 map 字面量会因键不规范化而查不到。
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(leaked)))
	h.Set("X-RateLimit-Limit", "60")
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", "1791699455")
	h.Set("Retry-After", "42")
	resp := &http.Response{
		StatusCode: http.StatusForbidden,
		Header:     h,
		Body:       body,
	}
	withFakeTransport(t, resp, body)

	c, w := newTestContext(http.MethodGet, "/https://api.github.com/repos/a/b")
	proxy(c, "https://api.github.com/repos/a/b", config.Load())

	if w.Code != http.StatusForbidden {
		t.Fatalf("状态码应保持 403，实际 %d", w.Code)
	}
	got := w.Body.String()
	if strings.Contains(got, "198.51.100.7") {
		t.Fatalf("上游出口 IP 被转发给客户端：%q", got)
	}
	if got != upstreamLimitedBody {
		t.Fatalf("响应体应为固定文案 %q，实际 %q", upstreamLimitedBody, got)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("应使用固定文案的 Content-Type，实际 %q", ct)
	}
	// 退避头部需保留（不含地址信息，对客户端有用）。
	if w.Header().Get("Retry-After") != "42" {
		t.Fatalf("Retry-After 应保留，实际 %q", w.Header().Get("Retry-After"))
	}
	if w.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("X-RateLimit-Remaining 应保留，实际 %q", w.Header().Get("X-RateLimit-Remaining"))
	}
	if w.Header().Get("X-RateLimit-Reset") != "1791699455" {
		t.Fatalf("X-RateLimit-Reset 应保留，实际 %q", w.Header().Get("X-RateLimit-Reset"))
	}
	// 上游的 Content-Length 描述的是被替换掉的正文，不能再转发。
	if cl := w.Header().Get("Content-Length"); cl != "" && cl != strconv.Itoa(w.Body.Len()) {
		t.Fatalf("Content-Length 与实际正文不一致：%q vs %d", cl, w.Body.Len())
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 次限流使用 429 + Retry-After，同样不应转发上游响应体。
func TestProxyHidesUpstream429Body(t *testing.T) {
	setTestGinMode(t)
	const leaked = `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes. (198.51.100.7)"}`
	body := &trackingBody{r: strings.NewReader(leaked)}
	resp := &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Content-Type": []string{"application/json; charset=utf-8"},
			"Retry-After":  []string{"60"},
		},
		Body: body,
	}
	withFakeTransport(t, resp, body)

	c, w := newTestContext(http.MethodGet, "/https://github.com/a/b/releases/download/v1/x.zip")
	proxy(c, "https://github.com/a/b/releases/download/v1/x.zip", config.Load())

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("状态码应保持 429，实际 %d", w.Code)
	}
	if w.Body.String() != upstreamLimitedBody {
		t.Fatalf("响应体应为固定文案，实际 %q", w.Body.String())
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 其余状态码维持原样转发，避免改变 API 客户端的既有语义。
func TestProxyStillForwardsNonLimitedErrorBodies(t *testing.T) {
	setTestGinMode(t)
	const payload = `{"message":"Not Found"}`
	body := &trackingBody{r: strings.NewReader(payload)}
	resp := &http.Response{
		StatusCode: http.StatusUnauthorized,
		Header:     http.Header{"Content-Type": []string{"application/json; charset=utf-8"}},
		Body:       body,
	}
	withFakeTransport(t, resp, body)

	c, w := newTestContext(http.MethodGet, "/https://api.github.com/repos/a/b")
	proxy(c, "https://api.github.com/repos/a/b", config.Load())

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("状态码应为 401，实际 %d", w.Code)
	}
	if w.Body.String() != payload {
		t.Fatalf("401 响应体应原样转发，实际 %q", w.Body.String())
	}
}
