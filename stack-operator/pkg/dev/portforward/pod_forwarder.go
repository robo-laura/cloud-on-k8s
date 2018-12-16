package portforward

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// podForwarder enables redirecting tcp connections through "kubectl port-forward" tooling
type podForwarder struct {
	network, addr      string
	podName, namespace string

	sync.Mutex

	// initChan is used to wait for the port-forwarder to be set up before redirecting connections
	initChan chan struct{}
	// viaErr is set when there's an error during initialization
	viaErr error
	// viaAddr is the address that we use when redirecting connections
	viaAddr string

	// ephemeralPortFinder is used to find an available ephemeral port
	ephemeralPortFinder func() (string, error)

	// portForwarderFactory is used to facilitate testing without using the API
	portForwarderFactory PortForwarderFactory

	// dialerFunc is used to facilitate testing without making new connections
	dialerFunc dialerFunc
}

var _ Forwarder = &podForwarder{}

// defaultEphemeralPortFinder finds an ephemral port by binding to :0 and checking what port was bound
var defaultEphemeralPortFinder = func() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	addr := listener.Addr().String()

	if err := listener.Close(); err != nil {
		return "", err
	}

	_, localPort, err := net.SplitHostPort(addr)

	return localPort, err
}

type PortForwarderFactory interface {
	NewPortForwarder(
		ctx context.Context,
		namespace, podName string,
		ports []string,
		readyChan chan struct{},
	) (PortForwarder, error)
}

type PortForwarder interface {
	ForwardPorts() error
}

// commandFactory is a factory for commands
type PortForwarderFactoryFunc func(
	ctx context.Context,
	namespace, podName string,
	ports []string,
	readyChan chan struct{},
) (PortForwarder, error)

func (f PortForwarderFactoryFunc) NewPortForwarder(
	ctx context.Context,
	namespace, podName string,
	ports []string,
	readyChan chan struct{},
) (PortForwarder, error) {
	return f(ctx, namespace, podName, ports, readyChan)
}

// defaultPortForwarderFactory is the default factory used for port forwarders outside of tests
var defaultPortForwarderFactory = PortForwarderFactoryFunc(newKubectlPortForwarder)

// dialerFunc is a factory for connections
type dialerFunc func(ctx context.Context, network, address string) (net.Conn, error)

// defaultDialerFunc is the default dialer function we use outside of tests
var defaultDialerFunc dialerFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

// NewPodForwarder returns a new initialized podForwarder
func NewPodForwarder(network, addr string) (*podForwarder, error) {
	podName, namespace, err := parsePodAddr(addr)
	if err != nil {
		return nil, err
	}

	return &podForwarder{
		network: network,
		addr:    addr,

		podName:   podName,
		namespace: namespace,

		initChan: make(chan struct{}),

		ephemeralPortFinder:  defaultEphemeralPortFinder,
		portForwarderFactory: defaultPortForwarderFactory,
		dialerFunc:           defaultDialerFunc,
	}, nil
}

// parsePodAddr parses the pod name and namespace from an address
func parsePodAddr(addr string) (string, string, error) {
	// (our) pods generally look like this (as FQDN): {name}.{namespace}.pod.cluster.local
	// TODO: subdomains in pod names would change this.
	parts := strings.SplitN(addr, ".", 3)

	if len(parts) <= 2 {
		return "", "", fmt.Errorf("unsupported pod address format: %s", addr)
	}

	name := parts[0]
	namespace := parts[1]
	return name, namespace, nil
}

// DialContext connects to the podForwarder address using the provided context.
func (f *podForwarder) DialContext(ctx context.Context) (net.Conn, error) {
	// wait until we're initialized or context is done
	select {
	case <-f.initChan:
	case <-ctx.Done():
	}

	// context has an error, so we can give up, most likely exceeded our timeout
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// we have an error to return
	if f.viaErr != nil {
		return nil, f.viaErr
	}

	log.Info("Redirecting dial call", "addr", f.addr, "via", f.viaAddr)
	return f.dialerFunc(ctx, f.network, f.viaAddr)
}

// Run starts a port forwarder and blocks until either the port forwarding fails or the context is done.
func (f *podForwarder) Run(ctx context.Context) error {
	log.Info("Running port-forwarder for", "addr", f.addr)
	defer log.Info("No longer running port-forwarder for", "addr", f.addr)

	// used as a safeguard to ensure we only close the init channel once
	initCloser := sync.Once{}

	// wrap this in a sync.Once because it will panic if it happens more than once
	// ensure that initChan is closed even if we were never ready.
	defer initCloser.Do(func() {
		close(f.initChan)
	})

	// derive a new context so we can ensure the port-forwarding is stopped before we return and that we return as
	// soon as the port-forwarding stops, whichever occurs first
	runCtx, runCtxCancel := context.WithCancel(ctx)
	defer runCtxCancel()

	_, port, err := net.SplitHostPort(f.addr)
	if err != nil {
		return err
	}

	// find an available local ephemeral port
	localPort, err := f.ephemeralPortFinder()
	if err != nil {
		return err
	}

	readyChan := make(chan struct{})
	fwd, err := f.portForwarderFactory.NewPortForwarder(
		runCtx,
		f.namespace, f.podName,
		[]string{localPort + ":" + port},
		readyChan,
	)
	if err != nil {
		return err
	}

	// wait for our context to be done or the port forwarder to become ready
	go func() {
		select {
		case <-runCtx.Done():
		case <-readyChan:
			f.viaAddr = "127.0.0.1:" + localPort

			log.Info("Ready to redirect connections", "addr", f.addr, "via", f.viaAddr)

			// wrap this in a sync.Once because it will panic if it happens more than once
			defer initCloser.Do(func() {
				close(f.initChan)
			})
		}
	}()

	err = fwd.ForwardPorts()
	f.viaErr = errors.New("not currently forwarding")
	return err
}

// newKubectlPortForwarder creates a new PortForwarder using kubectl tooling
func newKubectlPortForwarder(
	ctx context.Context,
	namespace, podName string,
	ports []string,
	readyChan chan struct{},
) (PortForwarder, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}

	clientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	req := clientSet.RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward")

	u := url.URL{
		Scheme:   req.URL().Scheme,
		Host:     req.URL().Host,
		Path:     "/api/v1" + req.URL().Path,
		RawQuery: "timeout=32s",
	}

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, &u)

	// wrap stdout / stderr through logging
	w := &logWriter{keysAndValues: []interface{}{
		"namespace", namespace,
		"pod", podName,
		"ports", ports,
	}}
	return portforward.New(dialer, ports, ctx.Done(), readyChan, w, w)
}

// logWriter is a small utility that writes data from an io.Writer to a log
type logWriter struct {
	keysAndValues []interface{}
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	log.Info(strings.TrimSpace(string(p)), w.keysAndValues...)

	return len(p), nil
}
