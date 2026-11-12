package checkflow

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// FaultPhase identifies at which point of the exchange a connection is cut.
type FaultPhase string

const (
	// FaultRequest cuts while the client request body is still travelling to
	// the service: the service can only ever observe a truncated request.
	FaultRequest FaultPhase = "request"
	// FaultResponse cuts after the service has reached a deterministic
	// storage-commit boundary but before the response is delivered, or after a
	// partial response has been delivered.
	FaultResponse FaultPhase = "response"
)

// Response cut modes for a FaultResponse fault.
const (
	// ResponseCutNone relays the response untouched (used by clean attempts).
	ResponseCutNone = "none"
	// ResponseCutBeforeAny closes the client socket the instant the configured
	// commit signal fires, before any response byte is relayed.
	ResponseCutBeforeAny = "before_any"
	// ResponseCutPartialBody relays the complete status line and headers and
	// then only the first ResponseBodyBytes bytes of the body before cutting.
	// The cut is armed by the configured commit signal so the handler cannot
	// have observed client cancellation beforehand.
	ResponseCutPartialBody = "partial_body"
)

// Trigger identifies a service-side storage boundary that can arm a cut.
type Trigger string

const (
	// TriggerSourceCommit fires when a source's media and metadata have been
	// atomically renamed into place.
	TriggerSourceCommit Trigger = "source_commit"
	// TriggerJobQueued fires when the durable queued render job is written.
	TriggerJobQueued Trigger = "job_queued"
	// TriggerRenderArtifacts fires when the output WAV and receipt folder has
	// been atomically renamed into place.
	TriggerRenderArtifacts Trigger = "render_artifacts_commit"
)

// Fault describes one deterministic connection cut.
//
// Request cuts are anchored to relayed byte counts so the service receives a
// genuinely truncated request. Response cuts are armed by an explicit service
// storage-boundary signal, which guarantees the cut happens after (or at) a
// known disk fact rather than racing the handler at an unknown point. No
// wall-clock timing is used.
type Fault struct {
	Phase             FaultPhase
	RequestForwarded  int
	ResponseMode      string
	ResponseBodyBytes int
	// ArmSignal, when non-nil, is closed by the instrumented repository once
	// the named commit boundary is reached. The proxy performs the response cut
	// when it observes the signal. It is excluded from evidence serialization.
	ArmSignal <-chan struct{} `json:"-"`
	// ArmTrigger labels the signal for the event log.
	ArmTrigger Trigger
}

// ProxyEvent is one ordered, machine-readable network fact recorded by the
// proxy. Events are the minimum evidence needed to explain an uncertain
// request outcome: they say exactly where the bytes stopped.
type ProxyEvent struct {
	Seq       int       `json:"seq"`
	Time      time.Time `json:"time"`
	Direction string    `json:"direction"`
	Bytes     int       `json:"bytes"`
	Note      string    `json:"note,omitempty"`
}

func (event ProxyEvent) String() string {
	detail := event.Note
	if detail == "" {
		detail = "-"
	}
	return fmt.Sprintf("#%02d %s %s bytes=%d %s", event.Seq, event.Time.Format("15:04:05.000000"), event.Direction, event.Bytes, detail)
}

// FaultProxy is a single-hop TCP relay in front of the real HTTP service. It
// speaks only raw TCP (it never terminates HTTP semantics itself), so the
// service sees genuine socket-level connection interruption.
type FaultProxy struct {
	listener net.Listener
	target   string
	fault    Fault

	mu         sync.Mutex
	events     []ProxyEvent
	closed     bool
	connDone   chan struct{}
	upDone     chan struct{}
	clientConn net.Conn
	armOnce    sync.Once
	armed      chan struct{}
}

// NewFaultProxy starts a proxy that forwards to targetAddress and applies the
// given fault to the first connection it accepts.
func NewFaultProxy(targetAddress string, fault Fault) (*FaultProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	proxy := &FaultProxy{
		listener: listener,
		target:   targetAddress,
		fault:    fault,
		connDone: make(chan struct{}),
		upDone:   make(chan struct{}),
		armed:    make(chan struct{}),
	}
	go proxy.serve()
	return proxy, nil
}

// Address is the dial address clients must use.
func (proxy *FaultProxy) Address() string {
	return proxy.listener.Addr().String()
}

// Events returns a snapshot of the ordered network facts observed so far.
func (proxy *FaultProxy) Events() []ProxyEvent {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	copied := make([]ProxyEvent, len(proxy.events))
	copy(copied, proxy.events)
	return copied
}

// UpstreamDone closes once the service-side connection reaches EOF. Waiting on
// this channel is an event-driven way to know the service handler has stopped
// writing; it replaces arbitrary sleep-based synchronization.
func (proxy *FaultProxy) UpstreamDone() <-chan struct{} {
	return proxy.upDone
}

// ConnectionDone closes once the whole relay attempt has torn down.
func (proxy *FaultProxy) ConnectionDone() <-chan struct{} {
	return proxy.connDone
}

// Close releases the listening socket.
func (proxy *FaultProxy) Close() error {
	proxy.mu.Lock()
	if proxy.closed {
		proxy.mu.Unlock()
		return nil
	}
	proxy.closed = true
	proxy.mu.Unlock()
	return proxy.listener.Close()
}

func (proxy *FaultProxy) record(direction string, bytes int, note string) {
	proxy.mu.Lock()
	proxy.events = append(proxy.events, ProxyEvent{
		Seq:       len(proxy.events) + 1,
		Time:      time.Now().UTC(),
		Direction: direction,
		Bytes:     bytes,
		Note:      note,
	})
	proxy.mu.Unlock()
}

// markArmed records that the service-side commit boundary was reached.
func (proxy *FaultProxy) markArmed() {
	proxy.armOnce.Do(func() {
		proxy.record("signal", 0, "commit boundary reached: "+string(proxy.fault.ArmTrigger))
		close(proxy.armed)
	})
}

func (proxy *FaultProxy) serve() {
	client, err := proxy.listener.Accept()
	if err != nil {
		close(proxy.connDone)
		return
	}
	proxy.mu.Lock()
	proxy.clientConn = client
	proxy.mu.Unlock()
	proxy.record("accept", 0, "client connection accepted")

	upstream, err := net.DialTimeout("tcp", proxy.target, 10*time.Second)
	if err != nil {
		proxy.record("dial", 0, "upstream dial failed: "+err.Error())
		_ = client.Close()
		close(proxy.upDone)
		close(proxy.connDone)
		return
	}
	proxy.record("dial", 0, "upstream connection established")

	if proxy.fault.Phase == FaultResponse && proxy.fault.ArmSignal != nil {
		go func() {
			select {
			case <-proxy.fault.ArmSignal:
				proxy.markArmed()
			case <-proxy.connDone:
			}
		}()
	}

	var relay sync.WaitGroup
	relay.Add(2)
	go func() {
		defer relay.Done()
		proxy.pumpRequest(client, upstream)
		if closer, ok := upstream.(*net.TCPConn); ok {
			_ = closer.CloseWrite()
		}
	}()
	go func() {
		defer relay.Done()
		proxy.pumpResponse(upstream, client)
	}()
	go func() {
		relay.Wait()
		_ = upstream.Close()
		_ = client.Close()
		close(proxy.connDone)
	}()
}

// pumpRequest relays the request stream and, in the request fault phase, cuts
// the client socket after the configured number of bytes.
func (proxy *FaultProxy) pumpRequest(client net.Conn, upstream net.Conn) {
	reader := bufio.NewReader(client)
	relayed := 0
	buffer := make([]byte, 1024)
	for {
		readLen := len(buffer)
		if proxy.fault.Phase == FaultRequest {
			remaining := proxy.fault.RequestForwarded - relayed
			if remaining <= 0 {
				proxy.record("request", relayed, "fault point reached: closing client connection")
				_ = client.Close()
				return
			}
			if remaining < readLen {
				readLen = remaining
			}
		}
		count, readErr := reader.Read(buffer[:readLen])
		if count > 0 {
			if _, writeErr := upstream.Write(buffer[:count]); writeErr != nil {
				proxy.record("request", relayed+count, "upstream write failed: "+writeErr.Error())
				return
			}
			relayed += count
			proxy.record("request", count, "")
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				proxy.record("request", relayed, "request stream end")
				return
			}
			proxy.record("request", relayed, "client read ended: "+readErr.Error())
			return
		}
	}
}

// pumpResponse relays the response stream, applying the response cut at the
// deterministic, signal-armed framing point. After cutting it drains the
// upstream socket to EOF so handler completion is observable without sleeping.
func (proxy *FaultProxy) pumpResponse(upstream net.Conn, client net.Conn) {
	defer close(proxy.upDone)
	reader := bufio.NewReader(upstream)

	if proxy.fault.Phase == FaultResponse && proxy.fault.ResponseMode == ResponseCutBeforeAny {
		if !proxy.waitArmed() {
			return
		}
		proxy.record("response", 0, "fault point reached: closing client before any response byte")
		_ = client.Close()
		proxy.drainUpstream(reader)
		return
	}

	headerBlock, headerErr := proxy.readHeadersWhenArmed(reader)
	if headerErr != nil {
		proxy.record("response", 0, "could not read response headers: "+headerErr.Error())
		return
	}
	if _, writeErr := client.Write(headerBlock); writeErr != nil {
		proxy.record("response", len(headerBlock), "client write failed while forwarding headers: "+writeErr.Error())
		proxy.drainUpstream(reader)
		return
	}
	proxy.record("response", len(headerBlock), "response headers relayed")

	if proxy.fault.Phase == FaultResponse && proxy.fault.ResponseMode == ResponseCutPartialBody {
		proxy.relayPartialBody(client, reader)
		proxy.drainUpstream(reader)
		return
	}
	proxy.relayBody(client, reader)
}

// waitArmed blocks until the service reaches the configured commit boundary.
// For faults without an arming signal it returns immediately.
func (proxy *FaultProxy) waitArmed() bool {
	if proxy.fault.Phase != FaultResponse || proxy.fault.ArmSignal == nil {
		return true
	}
	select {
	case <-proxy.armed:
		return true
	case <-proxy.connDone:
		return false
	}
}

// readHeadersWhenArmed delays header relay until the commit boundary is armed
// for the before-any cut, ensuring the handler never observes a premature
// client cancellation.
func (proxy *FaultProxy) readHeadersWhenArmed(reader *bufio.Reader) ([]byte, error) {
	if proxy.fault.Phase == FaultResponse && proxy.fault.ResponseMode == ResponseCutBeforeAny {
		if !proxy.waitArmed() {
			return nil, errors.New("relay tore down before the cut was armed")
		}
	}
	return readHTTPHeaders(reader)
}

// relayPartialBody forwards only the configured number of body bytes and then
// cuts the client connection. The arming signal must already have fired before
// the response headers were relayed.
func (proxy *FaultProxy) relayPartialBody(client net.Conn, reader *bufio.Reader) {
	if !proxy.waitArmed() {
		return
	}
	remaining := proxy.fault.ResponseBodyBytes
	buffer := make([]byte, 1024)
	for remaining > 0 {
		readLen := len(buffer)
		if remaining < readLen {
			readLen = remaining
		}
		count, readErr := reader.Read(buffer[:readLen])
		if count > 0 {
			if _, writeErr := client.Write(buffer[:count]); writeErr != nil {
				proxy.record("response", count, "client write failed during partial body: "+writeErr.Error())
				return
			}
			remaining -= count
			proxy.record("response", count, "partial body byte relayed")
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				proxy.record("response", 0, "upstream ended before partial-body cut point")
				return
			}
			proxy.record("response", 0, "upstream read ended during partial body: "+readErr.Error())
			return
		}
	}
	proxy.record("response", proxy.fault.ResponseBodyBytes, "fault point reached: closing client after partial body")
	_ = client.Close()
}

// relayBody forwards the remainder of the response without cutting.
func (proxy *FaultProxy) relayBody(client net.Conn, reader *bufio.Reader) {
	buffer := make([]byte, 2048)
	for {
		count, readErr := reader.Read(buffer)
		if count > 0 {
			if _, writeErr := client.Write(buffer[:count]); writeErr != nil {
				proxy.record("response", count, "client write failed: "+writeErr.Error())
				return
			}
			proxy.record("response", count, "")
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				proxy.record("response", 0, "response stream end")
				return
			}
			proxy.record("response", 0, "upstream read ended: "+readErr.Error())
			return
		}
	}
}

// drainUpstream consumes and discards the remaining upstream response so the
// service can finish its write path. Its terminal read marks handler
// completion through the UpstreamDone channel.
func (proxy *FaultProxy) drainUpstream(reader *bufio.Reader) {
	buffer := make([]byte, 2048)
	drained := 0
	for {
		count, readErr := reader.Read(buffer)
		drained += count
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				proxy.record("drain", drained, "service handler finished (upstream EOF)")
				return
			}
			proxy.record("drain", drained, "upstream drain ended: "+readErr.Error())
			return
		}
	}
}

// readHTTPHeaders reads through the terminating CRLFCRLF marker and returns the
// exact header block bytes.
func readHTTPHeaders(reader *bufio.Reader) ([]byte, error) {
	var block []byte
	suffix := make([]byte, 0, 4)
	for {
		byteValue, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		block = append(block, byteValue)
		suffix = append(suffix, byteValue)
		if len(suffix) > 4 {
			suffix = suffix[len(suffix)-4:]
		}
		if len(block) >= 4 && string(suffix) == "\r\n\r\n" {
			return block, nil
		}
		if len(block) > 1<<20 {
			return nil, errors.New("response headers exceed 1 MiB")
		}
	}
}
