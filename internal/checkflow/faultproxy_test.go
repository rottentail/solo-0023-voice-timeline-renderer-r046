package checkflow

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// startEchoHTTP serves a fixed JSON response on a real TCP listener and
// optionally blocks the response until the supplied boundary channel fires.
func startEchoHTTP(t *testing.T, boundary <-chan struct{}, body string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		go func() {
			defer conn.Close()
			reader := bufio.NewReader(conn)
			for {
				if _, readErr := http.ReadRequest(reader); readErr != nil {
					return
				}
				if boundary != nil {
					<-boundary
				}
				fmt.Fprintf(conn, "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
				return
			}
		}()
	}()
	return strings.TrimPrefix(listener.Addr().String(), "http://")
}

// startCountingOrigin reads exactly one full request (until EOF) and reports
// how many total request bytes reached it. Used to prove a request-cut relay
// forwards strictly fewer bytes than the client sent.
func startCountingOrigin(t *testing.T, received chan<- int) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		go func() {
			defer conn.Close()
			total := 0
			buffer := make([]byte, 4096)
			for {
				count, readErr := conn.Read(buffer)
				total += count
				if readErr != nil {
					received <- total
					return
				}
			}
		}()
	}()
	return strings.TrimPrefix(listener.Addr().String(), "http://")
}

func doRawRequest(t *testing.T, address string) (*http.Response, error) {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := fmt.Fprintf(conn, "PUT /resource HTTP/1.1\r\nHost: example\r\nContent-Length: 11\r\n\r\nhello world"); err != nil {
		t.Fatalf("write request: %v", err)
	}
	return http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "PUT"})
}

func TestFaultProxyCleanRelay(t *testing.T) {
	target := startEchoHTTP(t, nil, `{"ok":true}`)
	proxy, err := NewFaultProxy(target, Fault{ResponseMode: ResponseCutNone})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	response, err := doRawRequest(t, proxy.Address())
	if err != nil {
		t.Fatalf("clean relay should deliver a full response, got error: %v", err)
	}
	payload, _ := io.ReadAll(response.Body)
	if string(payload) != `{"ok":true}` {
		t.Fatalf("relayed body mismatch: %q", payload)
	}
	select {
	case <-proxy.UpstreamDone():
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never settled after clean relay")
	}
}

func TestFaultProxyRequestTruncation(t *testing.T) {
	const (
		cutAt     = 4096
		totalBody = 64 * 1024
	)
	originReceived := make(chan int, 1)
	target := startCountingOrigin(t, originReceived)
	proxy, err := NewFaultProxy(target, Fault{Phase: FaultRequest, RequestForwarded: cutAt})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	conn, err := net.Dial("tcp", proxy.Address())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	body := strings.Repeat("a", totalBody)
	requestText := fmt.Sprintf("PUT /resource HTTP/1.1\r\nHost: example\r\nContent-Length: %d\r\n\r\n%s", totalBody, body)
	if _, err := conn.Write([]byte(requestText)); err != nil {
		// A write error is also acceptable evidence that the client socket was cut.
		t.Logf("client write ended: %v", err)
	}

	// The client side must end with a closed/erroring socket rather than a
	// response, because the proxy cuts it at the request fault point.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 64)
	if n, readErr := conn.Read(buffer); readErr == nil && n > 0 {
		t.Fatalf("expected the client connection to be cut, got %d response bytes", n)
	}
	_ = conn.Close()

	select {
	case total := <-originReceived:
		if total >= len(requestText) {
			t.Fatalf("origin received the full %d-byte request; expected a truncated stream, got %d", len(requestText), total)
		}
		if total == 0 {
			t.Fatal("origin received no request bytes at all")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("origin never observed the end of the truncated request")
	}

	sawCutNote := false
	for _, event := range proxy.Events() {
		if strings.Contains(event.Note, "fault point reached") {
			sawCutNote = true
		}
	}
	if !sawCutNote {
		t.Fatal("proxy event log missing the fault point note")
	}
}

func TestFaultProxyBeforeAnyWaitsForCommitSignal(t *testing.T) {
	boundary := make(chan struct{})
	target := startEchoHTTP(t, boundary, "{}")
	arm := make(chan struct{})
	proxy, err := NewFaultProxy(target, Fault{
		Phase:        FaultResponse,
		ResponseMode: ResponseCutBeforeAny,
		ArmSignal:    arm,
		ArmTrigger:   TriggerSourceCommit,
	})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	conn, err := net.Dial("tcp", proxy.Address())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "PUT /resource HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n{}")

	// Before the commit signal fires, no response byte must reach the client.
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buffer := make([]byte, 16)
	if n, readErr := conn.Read(buffer); readErr == nil {
		t.Fatalf("proxy leaked %d response bytes before the commit signal", n)
	}

	// Service reaches its commit boundary; both the service-side signal and the
	// echo server's response gate now open.
	close(arm)
	close(boundary)

	select {
	case <-proxy.UpstreamDone():
	case <-time.After(5 * time.Second):
		t.Fatal("proxy never observed handler completion")
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, readErr := conn.Read(buffer); readErr == nil {
		t.Fatal("expected connection cut before any response byte after arming")
	}
}

func TestFaultProxyPartialBodyCut(t *testing.T) {
	boundary := make(chan struct{})
	body := `{"digest":"0123456789abcdef"}`
	target := startEchoHTTP(t, boundary, body)
	arm := make(chan struct{})
	proxy, err := NewFaultProxy(target, Fault{
		Phase:             FaultResponse,
		ResponseMode:      ResponseCutPartialBody,
		ResponseBodyBytes: 6,
		ArmSignal:         arm,
		ArmTrigger:        TriggerRenderArtifacts,
	})
	if err != nil {
		t.Fatalf("proxy: %v", err)
	}
	t.Cleanup(func() { _ = proxy.Close() })

	conn, err := net.Dial("tcp", proxy.Address())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(conn, "PUT /renders/x HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n{}")
	close(arm)
	close(boundary)

	reader := bufio.NewReader(conn)
	response, readErr := http.ReadResponse(reader, &http.Request{Method: "PUT"})
	if readErr != nil {
		t.Fatalf("headers should arrive intact: %v", readErr)
	}
	if response.StatusCode != 201 {
		t.Fatalf("unexpected status %d", response.StatusCode)
	}
	partial, readErr := io.ReadAll(response.Body)
	if readErr == nil {
		t.Fatalf("expected a truncated body, read full %q", partial)
	}
	if string(partial) != body[:6] {
		t.Fatalf("expected first 6 body bytes %q, got %q", body[:6], partial)
	}
}
