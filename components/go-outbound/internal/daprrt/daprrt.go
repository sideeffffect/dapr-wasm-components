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
	"os"
	"reflect"
	"sync"
	"unsafe"

	"github.com/dapr/dapr/cmd/daprd/options"
	daprgrpc "github.com/dapr/dapr/pkg/api/grpc"
	"github.com/dapr/dapr/pkg/api/grpc/manager"
	"github.com/dapr/dapr/pkg/channel"
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
	"github.com/dapr/dapr/pkg/config"
	"github.com/dapr/dapr/pkg/healthz"
	"github.com/dapr/dapr/pkg/metrics"
	"github.com/dapr/dapr/pkg/modes"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/dapr/pkg/runtime/channels"
	"github.com/dapr/dapr/pkg/runtime/registry"
	"github.com/dapr/dapr/pkg/security"
	"github.com/dapr/kit/concurrency"
	"github.com/dapr/kit/logger"
	"google.golang.org/grpc"

	_ "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/components" // component registrations

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/wasisock"
)

var (
	log        = logger.NewLogger("dapr.runtime")
	logContrib = logger.NewLogger("dapr.contrib")
)

// AppIP and AppPort are the app address Dapr is given. The app is reached
// with Go calls (App), not through it, but Dapr still uses it: name
// resolution registers the app port (Consul requires one), and Dapr waits
// for a TCP connect to it before it delivers anything. wasisock accepts that
// connect in-process; no byte ever flows over it.
var (
	AppIP   = net.IPv4(127, 11, 11, 11)
	AppPort = 80
)

// App is how the runtime reaches the attached app: plain Go calls, no app
// protocol on the wire (see package appcallback).
type App struct {
	// Callbacks serves the `AppCallback` gRPC service in-process: Dapr
	// calls it for pub/sub, input bindings and their subscription lists.
	Callbacks grpc.ClientConnInterface
	// Channel is Dapr's app channel: invocation, jobs, actors, health.
	Channel channel.AppChannel
}

// defaultArgs come before the user's arguments, so the user can override
// them (daprd's flag parser lets the last occurrence win).
var defaultArgs = []string{
	"--mode", string(modes.StandaloneMode),
	"--enable-metrics=false",
}

var (
	mu            sync.Mutex
	started       bool
	attachedStart bool
	app           *App // nil when outbound only
	rt            *runtime.DaprRuntime
	ready         = make(chan struct{}) // closed once rt is set (or failed)
	failed        error

	appID, namespace string // from the parsed arguments / NAMESPACE env
)

// IsSelf reports whether target ("<app-id>" or "<app-id>.<namespace>")
// names this runtime's own app.
func IsSelf(target string) bool {
	mu.Lock()
	defer mu.Unlock()
	if appID == "" {
		return false
	}
	ns := namespace
	if ns == "" {
		ns = "default"
	}
	return target == appID || target == appID+"."+ns
}

// StartAttached boots the runtime connected to a (the go-inbound pump is
// attached). It reports false if the runtime was already started without
// an app.
func StartAttached(a *App) bool {
	mu.Lock()
	defer mu.Unlock()
	if started {
		return attachedStart
	}
	attachedStart = true
	start(a)
	return true
}

// start boots the runtime; mu must be held. With an app, the runtime runs
// in gRPC app-protocol mode and reaches the app through a's Go objects (see
// attach). Without one it runs like daprd without --app-port (outbound
// only).
func start(a *App) {
	started = true
	app = a
	checkFields()

	args := append(append([]string{}, defaultArgs...), os.Args[1:]...)
	if a != nil {
		wasisock.RegisterVirtual(AppIP, AppPort, func() net.Conn {
			client, server := net.Pipe()
			_ = server.Close() // a connect probe only
			return client
		})
		// Last occurrence wins: the app is in-process, whatever the user
		// passed.
		args = append(args,
			"--app-port", fmt.Sprint(AppPort),
			"--app-channel-address", AppIP.String(),
			"--app-protocol", "grpc",
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
	mu.Lock()
	if !started {
		start(nil)
	}
	mu.Unlock()
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
	return *field[T](r, name)
}

// field returns a pointer to the unexported field name of the struct *p.
func field[T any, S any](p *S, name string) *T {
	f := reflect.ValueOf(p).Elem().FieldByName(name)
	return (*T)(unsafe.Pointer(f.UnsafeAddr()))
}

func checkFields() {
	for _, c := range []struct {
		t    reflect.Type
		name string
		want reflect.Type
	}{
		{reflect.TypeOf(runtime.DaprRuntime{}), "initComplete", reflect.TypeOf(make(chan struct{}))},
		{reflect.TypeOf(runtime.DaprRuntime{}), "daprGRPCAPI", reflect.TypeOf((*daprgrpc.API)(nil)).Elem()},
		{reflect.TypeOf(runtime.DaprRuntime{}), "grpc", reflect.TypeOf((*manager.Manager)(nil))},
		{reflect.TypeOf(runtime.DaprRuntime{}), "channels", reflect.TypeOf((*channels.Channels)(nil))},
		{reflect.TypeOf(channels.Channels{}), "appChannel", reflect.TypeOf((*channel.AppChannel)(nil)).Elem()},
		{reflect.TypeOf(channels.Channels{}), "appConnectionConfig", reflect.TypeOf(config.AppConnectionConfig{})},
	} {
		f, ok := c.t.FieldByName(c.name)
		if !ok || f.Type != c.want {
			panic(fmt.Sprintf("dapr field %v.%s (%v) not found: incompatible github.com/dapr/dapr version", c.t, c.name, c.want))
		}
	}
}

// attach connects a freshly built runtime, not yet running, to the app.
// DaprRuntime has no public way to take an app channel or an app gRPC
// connection, so both are set through unexported fields (checked by
// checkFields):
//   - the gRPC manager's app connection, which serves every `AppCallback`
//     call. Manager.SetAppClientConn is public (meant for tests);
//   - Channels.appChannel. Channels keeps its own copy of the app
//     connection config; with its app port zeroed, Channels.Refresh leaves
//     the channel untouched instead of building Dapr's own (it only warns,
//     so its logger is raised to errors in run).
func attach(r *runtime.DaprRuntime, a *App) {
	(*field[*manager.Manager](r, "grpc")).SetAppClientConn(a.Callbacks)
	ch := *field[*channels.Channels](r, "channels")
	field[config.AppConnectionConfig](ch, "appConnectionConfig").Port = 0
	*field[channel.AppChannel](ch, "appChannel") = a.Channel
}

// run mirrors daprd's cmd/daprd/app.runWithContext.
func run(args []string) error {
	opts, err := options.New(args)
	if err != nil {
		return fmt.Errorf("parsing daprd arguments: %w", err)
	}
	mu.Lock()
	appID, namespace = opts.AppID, os.Getenv("NAMESPACE")
	mu.Unlock()
	opts.Logger.SetAppID(opts.AppID)
	if err := logger.ApplyOptionsToLoggers(&opts.Logger); err != nil {
		return err
	}
	if app != nil {
		// Its only warning, "App channel is not initialized. Did you
		// configure an app-port?", is wrong here: attach sets the channel.
		logger.NewLogger("dapr.runtime.channels").SetOutputLevel(logger.ErrorLevel)
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
			if app != nil {
				attach(r, app)
			}
			mu.Lock()
			rt = r
			close(ready)
			mu.Unlock()
			return r.Run(ctx)
		},
	).Run(ctx)
}
