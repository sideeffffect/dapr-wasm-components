// Package daprrt boots the embedded Dapr runtime — the same code daprd runs
// (github.com/dapr/dapr/pkg/runtime) — inside this component, and hands out
// its in-process API object.
//
// Configuration is daprd's own command line: the component reads its
// arguments (wasi:cli/environment) and parses them with daprd's flag parser,
// so `wasmtime run composed.wasm --app-id orders --resources-path /resources`
// configures it exactly like `daprd --app-id orders --resources-path ...`.
package daprrt

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"sync"
	"unsafe"

	"github.com/dapr/dapr/cmd/daprd/options"
	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	bindingsLoader "github.com/dapr/dapr/pkg/components/bindings"
	configurationLoader "github.com/dapr/dapr/pkg/components/configuration"
	conversationLoader "github.com/dapr/dapr/pkg/components/conversation"
	cryptoLoader "github.com/dapr/dapr/pkg/components/crypto"
	lockLoader "github.com/dapr/dapr/pkg/components/lock"
	httpMiddlewareLoader "github.com/dapr/dapr/pkg/components/middleware/http"
	nrLoader "github.com/dapr/dapr/pkg/components/nameresolution"
	pubsubLoader "github.com/dapr/dapr/pkg/components/pubsub"
	secretstoresLoader "github.com/dapr/dapr/pkg/components/secretstores"
	stateLoader "github.com/dapr/dapr/pkg/components/state"
	"github.com/dapr/dapr/pkg/healthz"
	"github.com/dapr/dapr/pkg/metrics"
	"github.com/dapr/dapr/pkg/modes"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/registry"
	"github.com/dapr/dapr/pkg/security"
	"github.com/dapr/kit/concurrency"
	"github.com/dapr/kit/logger"
	_ "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/components" // component registrations

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/wasisock"
)

var (
	log        = logger.NewLogger("dapr.runtime")
	logContrib = logger.NewLogger("dapr.contrib")
)

// AppChannelIP and AppChannelPort are the virtual address of the app
// channel: Dapr's HTTP app protocol is served by an in-process handler,
// reached over in-memory pipes (wasisock.RegisterVirtual) — no socket.
var (
	AppChannelIP   = net.IPv4(127, 11, 11, 11)
	AppChannelPort = 80
)

// defaultArgs come before the user's arguments, so the user can override
// them (daprd's flag parser lets the last occurrence win).
var defaultArgs = []string{
	"--mode", string(modes.StandaloneMode),
	"--enable-metrics=false",
}

var (
	mu      sync.Mutex
	started bool
	rt      *runtime.DaprRuntime
	ready   = make(chan struct{}) // closed once rt is set (or failed)
	failed  error
)

// Start boots the runtime once. With an app handler (the go-inbound pump
// attached), Dapr's app channel is enabled and served by handler; without
// one the runtime runs like daprd without --app-port (outbound only).
// Later calls are no-ops.
func Start(handler http.Handler) {
	mu.Lock()
	defer mu.Unlock()
	if started {
		return
	}
	started = true
	checkFields()

	args := append(append([]string{}, defaultArgs...), os.Args[1:]...)
	if handler != nil {
		serveAppChannel(handler)
		args = append(args,
			"--app-port", fmt.Sprint(AppChannelPort),
			"--app-channel-address", AppChannelIP.String(),
			"--app-protocol", "http",
		)
	}
	go func() {
		err := run(args)
		mu.Lock()
		defer mu.Unlock()
		if failed == nil {
			if err == nil {
				err = fmt.Errorf("dapr runtime stopped")
			}
			failed = err
		}
		select {
		case <-ready:
		default:
			close(ready)
		}
	}()
}

// API returns the runtime's in-process API object, starting the runtime
// (outbound only) if nothing started it yet and blocking until it has
// initialized. A runtime that failed to start is an unrecoverable
// infrastructure failure: API panics (the component traps).
func API() daprgrpc.API {
	Start(nil)
	<-ready
	mu.Lock()
	r, err := rt, failed
	mu.Unlock()
	if r == nil {
		panic(fmt.Sprintf("dapr runtime failed to start: %v", err))
	}
	<-unexported[chan struct{}](r, "initComplete")
	return unexported[daprgrpc.API](r, "daprGRPCAPI")
}

// unexported reads an unexported field of the runtime. DaprRuntime has no
// public accessor for its API object; the field names are checked against
// the pinned Dapr version by checkFields at start, so a Dapr upgrade that
// renames them fails loudly instead of misbehaving.
func unexported[T any](r *runtime.DaprRuntime, name string) T {
	f := reflect.ValueOf(r).Elem().FieldByName(name)
	return *(*T)(unsafe.Pointer(f.UnsafeAddr()))
}

func checkFields() {
	t := reflect.TypeOf(runtime.DaprRuntime{})
	for name, want := range map[string]reflect.Type{
		"initComplete": reflect.TypeOf(make(chan struct{})),
		"daprGRPCAPI":  reflect.TypeOf((*daprgrpc.API)(nil)).Elem(),
	} {
		f, ok := t.FieldByName(name)
		if !ok || f.Type != want {
			panic(fmt.Sprintf("dapr runtime field %s (%v) not found: incompatible github.com/dapr/dapr version", name, want))
		}
	}
}

// pipeListener is a net.Listener fed with the server ends of net.Pipe
// connections created by virtual dials.
type pipeListener struct{ conns chan net.Conn }

func (l *pipeListener) Accept() (net.Conn, error) {
	c, ok := <-l.conns
	if !ok {
		return nil, net.ErrClosed
	}
	return c, nil
}
func (l *pipeListener) Close() error { return nil }
func (l *pipeListener) Addr() net.Addr {
	return &net.TCPAddr{IP: AppChannelIP, Port: AppChannelPort}
}

func serveAppChannel(handler http.Handler) {
	ln := &pipeListener{conns: make(chan net.Conn, 64)}
	wasisock.RegisterVirtual(AppChannelIP, AppChannelPort, func() net.Conn {
		client, server := net.Pipe()
		ln.conns <- server
		return client
	})
	go func() {
		// Serve only returns on listener failure, which pipeListener never has.
		_ = (&http.Server{Handler: handler}).Serve(ln)
	}()
}

// run mirrors daprd's cmd/daprd/app.runWithContext.
func run(args []string) error {
	opts, err := options.New(args)
	if err != nil {
		return fmt.Errorf("parsing daprd arguments: %w", err)
	}
	opts.Logger.SetAppID(opts.AppID)
	if err := logger.ApplyOptionsToLoggers(&opts.Logger); err != nil {
		return err
	}

	secretstoresLoader.DefaultRegistry.Logger = logContrib
	stateLoader.DefaultRegistry.Logger = logContrib
	cryptoLoader.DefaultRegistry.Logger = logContrib
	configurationLoader.DefaultRegistry.Logger = logContrib
	lockLoader.DefaultRegistry.Logger = logContrib
	pubsubLoader.DefaultRegistry.Logger = logContrib
	nrLoader.DefaultRegistry.Logger = logContrib
	bindingsLoader.DefaultRegistry.Logger = logContrib
	conversationLoader.DefaultRegistry.Logger = logContrib
	httpMiddlewareLoader.DefaultRegistry.Logger = log

	reg := registry.NewOptions().
		WithSecretStores(secretstoresLoader.DefaultRegistry).
		WithStateStores(stateLoader.DefaultRegistry).
		WithConfigurations(configurationLoader.DefaultRegistry).
		WithLocks(lockLoader.DefaultRegistry).
		WithPubSubs(pubsubLoader.DefaultRegistry).
		WithNameResolutions(nrLoader.DefaultRegistry).
		WithBindings(bindingsLoader.DefaultRegistry).
		WithCryptoProviders(cryptoLoader.DefaultRegistry).
		WithHTTPMiddlewares(httpMiddlewareLoader.DefaultRegistry).
		WithConversations(conversationLoader.DefaultRegistry)

	ctx := context.Background()
	hz := healthz.New()
	secProvider, err := security.New(ctx, security.Options{
		SentryAddress:           opts.SentryAddress,
		ControlPlaneTrustDomain: opts.ControlPlaneTrustDomain,
		ControlPlaneNamespace:   opts.ControlPlaneNamespace,
		TrustAnchors:            opts.TrustAnchors,
		AppID:                   opts.AppID,
		MTLSEnabled:             opts.EnableMTLS,
		Mode:                    modes.DaprMode(opts.Mode),
		Healthz:                 hz,
		JwtAudiences:            opts.SentryRequestJwtAudiences,
	})
	if err != nil {
		return err
	}

	return concurrency.NewRunnerManager(
		secProvider.Run,
		func(ctx context.Context) error {
			sec, serr := secProvider.Handler(ctx)
			if serr != nil {
				return serr
			}
			meter := metrics.NewMeter()
			defer meter.Stop()

			r, rerr := runtime.FromConfig(ctx, &runtime.Config{
				AppID:                         opts.AppID,
				ActorsService:                 opts.ActorsService,
				ActorsDisseminationTimeout:    opts.ActorsDisseminationTimeout,
				HotReloadReconcileInterval:    opts.HotReloadReconcileInterval,
				RemindersService:              opts.RemindersService,
				SchedulerAddress:              opts.SchedulerAddress,
				SchedulerStreams:              opts.SchedulerJobStreams,
				AllowedOrigins:                opts.AllowedOrigins,
				ResourcesPath:                 opts.ResourcesPath,
				ControlPlaneAddress:           opts.ControlPlaneAddress,
				AppProtocol:                   opts.AppProtocol,
				Mode:                          opts.Mode,
				DaprHTTPPort:                  opts.DaprHTTPPort,
				DaprInternalGRPCPort:          opts.DaprInternalGRPCPort,
				DaprInternalGRPCListenAddress: opts.DaprInternalGRPCListenAddress,
				DaprAPIGRPCPort:               opts.DaprAPIGRPCPort,
				DaprAPIListenAddresses:        opts.DaprAPIListenAddresses,
				DaprPublicPort:                opts.DaprPublicPort,
				DaprPublicListenAddress:       opts.DaprPublicListenAddress,
				ApplicationPort:               opts.AppPort,
				ProfilePort:                   opts.ProfilePort,
				EnableProfiling:               opts.EnableProfiling,
				AppMaxConcurrency:             opts.AppMaxConcurrency,
				EnableMTLS:                    opts.EnableMTLS,
				SentryAddress:                 opts.SentryAddress,
				MaxRequestSize:                opts.MaxRequestSize,
				ReadBufferSize:                opts.ReadBufferSize,
				UnixDomainSocket:              opts.UnixDomainSocket,
				DaprGracefulShutdownSeconds:   opts.DaprGracefulShutdownSeconds,
				DaprBlockShutdownDuration:     opts.DaprBlockShutdownDuration,
				DisableBuiltinK8sSecretStore:  opts.DisableBuiltinK8sSecretStore,
				EnableAppHealthCheck:          opts.EnableAppHealthCheck,
				AppHealthCheckPath:            opts.AppHealthCheckPath,
				AppHealthProbeInterval:        opts.AppHealthProbeInterval,
				AppHealthProbeTimeout:         opts.AppHealthProbeTimeout,
				AppHealthThreshold:            opts.AppHealthThreshold,
				AppChannelAddress:             opts.AppChannelAddress,
				EnableAPILogging:              opts.EnableAPILogging,
				Config:                        opts.Config,
				DisableInitEndpoints:          opts.DisableInitEndpoints,
				AppBindingOptionsTimeout:      opts.AppBindingOptionsTimeout,
				Metrics: metrics.Options{
					Enabled:       opts.Metrics.Enabled(),
					Log:           log,
					Port:          opts.Metrics.Port(),
					Namespace:     metrics.DefaultMetricNamespace,
					Healthz:       hz,
					ListenAddress: opts.Metrics.ListenAddress(),
					Meter:         meter,
				},
				AppSSL:         opts.AppSSL,
				ComponentsPath: opts.ComponentsPath,
				Registry:       reg,
				Security:       sec,
				Healthz:        hz,
			})
			if rerr != nil {
				return rerr
			}
			mu.Lock()
			rt = r
			close(ready)
			mu.Unlock()
			return r.Run(ctx)
		},
	).Run(ctx)
}
