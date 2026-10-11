package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"text/template"

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

// ---------------------------------------------------------------------------
// 出口 IP 脱敏：只对「发往 api.github.com 且状态码 403 / 429」的响应做**精准**
// 替换 —— 仅把 message 字段里 "API rate limit exceeded for <ip>." 的地址换成
// "this proxy Server"，其余字段（含 documentation_url）、提示文字与状态码全部
// 保留。任何条件不满足都按原样转发。
//
// 测试统一使用文档保留段地址 198.51.100.7（TEST-NET-2），不写入生产真实出口 IP。
// ---------------------------------------------------------------------------

const (
	redactTestIP        = "198.51.100.7"
	redactTestSuffix    = " (But here's the good news: Authenticated requests get a higher rate limit. Check out the documentation for more details.)"
	redactTestDocURL    = "https://docs.github.com/rest/overview/resources-in-the-rest-api#rate-limiting"
	redactTestBody      = `{"message":"API rate limit exceeded for ` + redactTestIP + `.` + redactTestSuffix + `","documentation_url":"` + redactTestDocURL + `"}`
	redactTestExpected  = `{"message":"API rate limit exceeded for this proxy Server.` + redactTestSuffix + `","documentation_url":"` + redactTestDocURL + `"}`
	redactTestAPITarget = "https://api.github.com/repos/a/b"
)

// jsonTestHeader 构造 JSON 响应头，附带常见限流头部。
func jsonTestHeader(bodyLen int) http.Header {
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(bodyLen))
	h.Set("X-RateLimit-Limit", "60")
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("Retry-After", "60")
	return h
}

// runProxyUpstream 让 proxy() 通过伪 transport 收到指定上游响应，并返回 recorder。
// resp 与上游 body 也一并返回，便于断言 Close 与 ContentLength。
func runProxyUpstream(t *testing.T, target string, status int, hdr http.Header, bodyText string) (*httptest.ResponseRecorder, *trackingBody) {
	t.Helper()
	setTestGinMode(t)
	body := &trackingBody{r: strings.NewReader(bodyText)}
	resp := &http.Response{
		StatusCode:    status,
		Header:        hdr,
		Body:          body,
		ContentLength: int64(len(bodyText)),
	}
	withFakeTransport(t, resp, body)
	c, w := newTestContext(http.MethodGet, "/"+target)
	proxy(c, target, config.Load())
	return w, body
}

// 1. 目标 403：IP 被替换成 this proxy Server，其余提示文字与 documentation_url
// 逐字节保留，状态码仍为 403。
func TestProxyRedactsGitHubAPIRateLimit403(t *testing.T) {
	w, body := runProxyUpstream(t, redactTestAPITarget, http.StatusForbidden,
		jsonTestHeader(len(redactTestBody)), redactTestBody)

	if w.Code != http.StatusForbidden {
		t.Fatalf("状态码应为 403，实际 %d", w.Code)
	}
	if got := w.Body.String(); got != redactTestExpected {
		t.Fatalf("正文应为精准替换后的原文：\n got %q\nwant %q", got, redactTestExpected)
	}
	if strings.Contains(w.Body.String(), redactTestIP) {
		t.Fatalf("出口 IP 仍出现在正文中：%q", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), redactTestDocURL) {
		t.Fatalf("documentation_url 必须保留：%q", w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "json") {
		t.Fatalf("Content-Type 应保持 JSON，实际 %q", ct)
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
	// Content-Length 必须与新正文一致，或干脆不下发。
	if cl := w.Header().Get("Content-Length"); cl != "" && cl != strconv.Itoa(w.Body.Len()) {
		t.Fatalf("Content-Length 与正文不符：头部 %q，正文 %d 字节", cl, w.Body.Len())
	}
	// 上游其它头部照常透传（本次改动不再裁剪头部）。
	if w.Header().Get("X-RateLimit-Remaining") != "0" || w.Header().Get("Retry-After") != "60" {
		t.Fatalf("上游头部应透传，实际 remaining=%q retry-after=%q",
			w.Header().Get("X-RateLimit-Remaining"), w.Header().Get("Retry-After"))
	}
}

// 2. 目标 429：message 符合目标格式时同样精准替换，状态码仍为 429。
func TestProxyRedactsGitHubAPIRateLimit429(t *testing.T) {
	w, body := runProxyUpstream(t, redactTestAPITarget, http.StatusTooManyRequests,
		jsonTestHeader(len(redactTestBody)), redactTestBody)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("状态码应为 429，实际 %d", w.Code)
	}
	if w.Body.String() != redactTestExpected {
		t.Fatalf("正文应精准替换，实际 %q", w.Body.String())
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 3. 普通 403 权限错误：正文完全保留。
func TestProxyKeepsPlain403Body(t *testing.T) {
	const plain = `{"message":"Must have admin rights to the repository.","documentation_url":"https://docs.github.com/rest"}`
	w, body := runProxyUpstream(t, redactTestAPITarget, http.StatusForbidden,
		jsonTestHeader(len(plain)), plain)

	if w.Code != http.StatusForbidden {
		t.Fatalf("状态码应为 403，实际 %d", w.Code)
	}
	if w.Body.String() != plain {
		t.Fatalf("普通 403 正文必须原样保留，实际 %q", w.Body.String())
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 4. 普通 429 次级限流（不含目标 IP 文案）：正文完全保留。
func TestProxyKeepsUnmatched429Body(t *testing.T) {
	const secondary = `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again.","documentation_url":"https://docs.github.com/rest/overview/resources-in-the-rest-api#secondary-rate-limits"}`
	w, _ := runProxyUpstream(t, redactTestAPITarget, http.StatusTooManyRequests,
		jsonTestHeader(len(secondary)), secondary)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("状态码应为 429，实际 %d", w.Code)
	}
	if w.Body.String() != secondary {
		t.Fatalf("未匹配文案的 429 正文必须原样保留，实际 %q", w.Body.String())
	}
}

// 5. 非 GitHub API 上游：文案相似也不做替换。
func TestProxyDoesNotRedactNonGitHubAPIHost(t *testing.T) {
	for _, target := range []string{
		"https://raw.githubusercontent.com/a/b/main/x.txt",
		"https://github.com/a/b/releases/download/v1/x.zip",
		"https://example.com/whatever",
	} {
		w, _ := runProxyUpstream(t, target, http.StatusForbidden,
			jsonTestHeader(len(redactTestBody)), redactTestBody)
		if w.Body.String() != redactTestBody {
			t.Fatalf("非 api.github.com 上游（%s）不应替换，实际 %q", target, w.Body.String())
		}
	}
}

// 6. 其他字段含 IPv4：只替换 message 中目标位置的地址，其它字段一律不动。
func TestProxyRedactsOnlyMessageIP(t *testing.T) {
	// {"message": <目标文案>, "documentation_url":..., "other_ip":"10.0.0.1", "note":"see 203.0.113.9"}
	other := `{"message":"API rate limit exceeded for ` + redactTestIP + `.` + redactTestSuffix + `","documentation_url":"` + redactTestDocURL + `","other_ip":"10.0.0.1","note":"see 203.0.113.9 for details"}`
	want := `{"message":"API rate limit exceeded for this proxy Server.` + redactTestSuffix + `","documentation_url":"` + redactTestDocURL + `","other_ip":"10.0.0.1","note":"see 203.0.113.9 for details"}`

	w, _ := runProxyUpstream(t, redactTestAPITarget, http.StatusForbidden, jsonTestHeader(len(other)), other)

	if got := w.Body.String(); got != want {
		t.Fatalf("只应替换 message 里的目标 IP：\n got %q\nwant %q", got, want)
	}
	if !strings.Contains(w.Body.String(), "10.0.0.1") || !strings.Contains(w.Body.String(), "203.0.113.9") {
		t.Fatalf("其它字段中的地址必须保留：%q", w.Body.String())
	}
}

// 7. 无效 JSON，或文案不匹配：保持原有转发行为。
func TestProxyKeepsUnparsableOrUnmatchedBody(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		// 截断的 JSON（有效 JSON 对象之外的情形）
		{"invalid-json", `{"message":"API rate limit exceeded for ` + redactTestIP + `. ` + redactTestSuffix},
		// 有效 JSON 但 message 不含目标前缀
		{"unmatched-message", `{"message":"API rate limit exceeded for ` + redactTestIP + `","documentation_url":"x"}`},
		// message 不是字符串
		{"non-string-message", `{"message":{"text":"API rate limit exceeded for ` + redactTestIP + `. x"},"documentation_url":"x"}`},
		// 顶层不是 JSON 对象
		{"non-object", `["API rate limit exceeded for ` + redactTestIP + `. x"]`},
		// 前缀后面不是合法 IPv4（999 段超范围）
		{"bad-ipv4", `{"message":"API rate limit exceeded for 999.1.1.1. x","documentation_url":"x"}`},
		// 地址后没有句点（不符合已知文案格式）
		{"no-trailing-dot", `{"message":"API rate limit exceeded for ` + redactTestIP + ` x","documentation_url":"x"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := runProxyUpstream(t, redactTestAPITarget, http.StatusForbidden,
				jsonTestHeader(len(tc.body)), tc.body)
			if got := w.Body.String(); got != tc.body {
				t.Fatalf("不满足脱敏条件时应原样转发：\n got %q\nwant %q", got, tc.body)
			}
		})
	}
}

// 8. 其他状态码：正文与状态码均不变（含正文里出现 IPv4 的情形）。
func TestProxyLeavesOtherStatusesUnchanged(t *testing.T) {
	const m = "API rate limit exceeded for " + redactTestIP + "."
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"401", http.StatusUnauthorized, `{"message":"Bad credentials"}`},
		{"422", http.StatusUnprocessableEntity, `{"message":"Validation Failed"}`},
		{"500", http.StatusInternalServerError, `{"message":"Server Error ` + m + `"}`},
		{"200", http.StatusOK, `{"ok":true,"note":"` + m + `"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackingBody{r: strings.NewReader(tc.body)}
			resp := &http.Response{StatusCode: tc.status, Header: jsonTestHeader(len(tc.body)), Body: body, ContentLength: int64(len(tc.body))}
			withFakeTransport(t, resp, body)
			c, w := newTestContext(http.MethodGet, "/"+redactTestAPITarget)
			proxy(c, redactTestAPITarget, config.Load())

			if w.Code != tc.status {
				t.Fatalf("状态码应为 %d，实际 %d", tc.status, w.Code)
			}
			if w.Body.String() != tc.body {
				t.Fatalf("非 403/429 的正文必须原样转发，实际 %q", w.Body.String())
			}
		})
	}
}

// 9. 404 仍走本站 404 页面（不经脱敏分支）。
func TestProxyKeeps404Behavior(t *testing.T) {
	setTestGinMode(t)
	old := tmpl404
	tmpl404 = template.Must(template.New("404").Parse("NOT_FOUND_PAGE"))
	t.Cleanup(func() { tmpl404 = old })

	const nf = `{"message":"Not Found","documentation_url":"https://docs.github.com/rest"}`
	body := &trackingBody{r: strings.NewReader(nf)}
	resp := &http.Response{StatusCode: http.StatusNotFound, Header: jsonTestHeader(len(nf)), Body: body, ContentLength: int64(len(nf))}
	withFakeTransport(t, resp, body)
	c, w := newTestContext(http.MethodGet, "/"+redactTestAPITarget)
	proxy(c, redactTestAPITarget, config.Load())

	if w.Code != http.StatusNotFound {
		t.Fatalf("状态码应为 404，实际 %d", w.Code)
	}
	if w.Body.String() != "NOT_FOUND_PAGE" {
		t.Fatalf("404 应渲染本站页面，实际 %q", w.Body.String())
	}
	if !body.closed {
		t.Fatal("上游响应体未被关闭")
	}
}

// 10. 脱敏函数本身：占位符与句点位置正确，且不动 documentation_url。
func TestRedactRateLimitMessage(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		changed bool
	}{
		{"API rate limit exceeded for 198.51.100.7. tail", "API rate limit exceeded for this proxy Server. tail", true},
		{"API rate limit exceeded for 1.2.3.4.", "API rate limit exceeded for this proxy Server.", true},
		{"API rate limit exceeded for 256.0.0.1. tail", "", false},   // 段超范围
		{"API rate limit exceeded for 010.0.0.1. tail", "", false},   // 前导零
		{"API rate limit exceeded for 198.51.100.7 tail", "", false}, // 缺句点
		{"Other message 198.51.100.7. tail", "", false},              // 前缀不匹配
		{"", "", false},
	}
	for _, tc := range cases {
		got, changed := redactRateLimitMessage(tc.in)
		if changed != tc.changed {
			t.Fatalf("输入 %q：changed=%v，期望 %v", tc.in, changed, tc.changed)
		}
		if changed && got != tc.want {
			t.Fatalf("输入 %q：\n got %q\nwant %q", tc.in, got, tc.want)
		}
	}
}
