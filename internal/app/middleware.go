package app

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/atomic"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/guid"
	"golang.org/x/xerrors"
)

const (
	// YT balancer's header.
	xReqIDHTTPHeader = "X-Req-Id"

	xForwardedForHTTPHeader  = "X-Forwarded-For"
	xForwardedForYHTTPHeader = "X-Forwarded-For-Y"
)

// requestLog logs
//   - http method, path, query, body
//   - generated guid
//   - balancer's request id (X-Req-Id header)
//   - request execution time, status and number of bytes written
//   - original client IP
func requestLog(l log.Structured) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := guid.New()
			requestIDField := log.String("request_id", requestID.String())

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			resp := &bytes.Buffer{}
			ww.Tee(resp)

			const bodySizeLimit = 1024 * 1024
			body, err := io.ReadAll(io.LimitReader(r.Body, bodySizeLimit))
			if err != nil {
				l.Error("error reading request body", log.Error(err))
			}
			r.Body = io.NopCloser(bytes.NewBuffer(body))

			l.Debug("HTTP request started",
				requestIDField,
				log.String("method", r.Method),
				log.String("path", r.URL.Path),
				log.String("query", r.Form.Encode()),
				log.ByteString("body", body),
				log.String("origin", Origin(r)),
				log.String("l7_req_id", r.Header.Get(xReqIDHTTPHeader)))

			t0 := time.Now()
			defer func() {
				l.Debug("HTTP request finished",
					requestIDField,
					log.Int("status", ww.Status()),
					log.Int("bytes", ww.BytesWritten()),
					log.ByteString("response", resp.Bytes()),
					log.Duration("duration", time.Since(t0)))
			}()

			ctx := ctxlog.WithFields(r.Context(), requestIDField)
			ctx = withRequestID(ctx, requestID)
			next.ServeHTTP(ww, r.WithContext(ctx))
		})
	}
}

func timeout(timeout time.Duration) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// requestIDKey is a key used to access request id in request's ctx.
var requestIDKey struct{}

// withRequestID copies given context and adds (*requestIDKey, reqID) to values.
func withRequestID(ctx context.Context, reqID guid.GUID) context.Context {
	return context.WithValue(ctx, &requestIDKey, reqID)
}

type LeaderWatcher interface {
	OtherLeaderIsActive() (url *url.URL, ok bool)
}

func ForwardToLeader(watcher LeaderWatcher, l log.Structured) func(next http.Handler) http.Handler {
	const noLeaderForward = "X-No-Leader-Forward"

	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			if r.Header.Get(noLeaderForward) != "" {
				next.ServeHTTP(w, r)
				return
			}

			leaderURL, ok := watcher.OtherLeaderIsActive()
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			r.Header.Add(noLeaderForward, "1")
			l.Info("forwarding request to the leader", log.String("leader_url", leaderURL.String()))
			proxy := httputil.NewSingleHostReverseProxy(leaderURL)
			proxy.ModifyResponse = func(rsp *http.Response) error {
				rsp.Header.Del("Access-Control-Allow-Origin")
				rsp.Header.Del("Access-Control-Allow-Credentials")
				return nil
			}

			proxy.ServeHTTP(w, r)
		}

		return http.HandlerFunc(fn)
	}
}

func waitReady(ready *atomic.Bool) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !ready.Load() {
				internalError(w, xerrors.New("not ready, try later"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
