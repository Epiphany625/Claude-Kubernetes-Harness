// Package config reads the service's configuration from the environment.
//
// Environment only, no config file: every value either comes from a ConfigMap or
// a Secret in Kubernetes, and a file would mean a third place for a value to
// come from. Every variable is prefixed ALERTPROCESSOR_ so `envFrom` a whole
// ConfigMap cannot collide with anything the runtime sets.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// EnvPrefix is prepended to every variable name this package reads.
const EnvPrefix = "ALERTPROCESSOR_"

// Config is the fully resolved configuration. Built only by Load.
type Config struct {
	HTTP     HTTPConfig
	Postgres PostgresConfig
	AMQP     AMQPConfig
	Log      LogConfig
}

type HTTPConfig struct {
	Addr        string
	WebhookPath string
	// MaxBodyBytes caps a single webhook payload. A node failure can alert on
	// hundreds of pods at once, so this is generous; it exists to bound memory,
	// not to be a policy.
	MaxBodyBytes    int64
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

type PostgresConfig struct {
	// DSN is the whole connection string. The discrete fields below override
	// their corresponding part of it when set, so a Secret can supply only the
	// password while the ConfigMap holds the rest.
	DSN            string
	Host           string
	Port           int
	User           string
	Password       string
	Database       string
	SSLMode        string
	MaxConns       int32
	MinConns       int32
	ConnectTimeout time.Duration
	QueryTimeout   time.Duration
	AutoMigrate    bool
}

type AMQPConfig struct {
	URL      string
	Host     string
	Port     int
	Username string
	Password string
	VHost    string

	Exchange        string
	Queue           string
	RoutingPrefix   string
	DeclareTopology bool
	ConfirmTimeout  time.Duration
	ConnectTimeout  time.Duration
	// PublishTimeout bounds one publish attempt end to end. It must stay well
	// under Alertmanager's webhook timeout or the sender gives up first and
	// retries work that was about to succeed.
	PublishTimeout time.Duration
}

type LogConfig struct {
	Level  string
	Format string // "json" or "text"
}

// Load reads and validates the configuration. It returns every problem it finds
// rather than the first, because a misconfigured deployment usually has more
// than one wrong variable and fixing them one restart at a time is miserable.
func Load() (*Config, error) {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	cfg := &Config{
		HTTP: HTTPConfig{
			Addr:            env("HTTP_ADDR", ":8080"),
			WebhookPath:     env("WEBHOOK_PATH", "/api/v1/alerts"),
			MaxBodyBytes:    envInt64("MAX_BODY_BYTES", 8<<20, &errs), // 8 MiB
			ReadTimeout:     envDuration("HTTP_READ_TIMEOUT", 15*time.Second, &errs),
			WriteTimeout:    envDuration("HTTP_WRITE_TIMEOUT", 30*time.Second, &errs),
			IdleTimeout:     envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second, &errs),
			ShutdownTimeout: envDuration("SHUTDOWN_TIMEOUT", 20*time.Second, &errs),
		},
		Postgres: PostgresConfig{
			DSN:            env("POSTGRES_DSN", ""),
			Host:           env("POSTGRES_HOST", ""),
			Port:           envInt("POSTGRES_PORT", 0, &errs),
			User:           env("POSTGRES_USER", ""),
			Password:       env("POSTGRES_PASSWORD", ""),
			Database:       env("POSTGRES_DATABASE", ""),
			SSLMode:        env("POSTGRES_SSLMODE", ""),
			MaxConns:       int32(envInt("POSTGRES_MAX_CONNS", 10, &errs)),
			MinConns:       int32(envInt("POSTGRES_MIN_CONNS", 0, &errs)),
			ConnectTimeout: envDuration("POSTGRES_CONNECT_TIMEOUT", 10*time.Second, &errs),
			QueryTimeout:   envDuration("POSTGRES_QUERY_TIMEOUT", 5*time.Second, &errs),
			AutoMigrate:    envBool("DB_AUTO_MIGRATE", true, &errs),
		},
		AMQP: AMQPConfig{
			URL:      "", // Derived from host, port, username, and password below.
			Host:     env("AMQP_HOST", ""),
			Port:     envInt("AMQP_PORT", 0, &errs),
			Username: env("AMQP_USERNAME", ""),
			Password: env("AMQP_PASSWORD", ""),
			VHost:    "/",
			// Topology names and timeouts are fixed service settings.
			Exchange:        "alerts",
			Queue:           "agent.events",
			RoutingPrefix:   "alert",
			DeclareTopology: true,
			ConfirmTimeout:  5 * time.Second,
			ConnectTimeout:  10 * time.Second,
			PublishTimeout:  8 * time.Second,
		},
		Log: LogConfig{
			Level:  strings.ToLower(env("LOG_LEVEL", "info")),
			Format: strings.ToLower(env("LOG_FORMAT", "json")),
		},
	}

	dsn, err := cfg.Postgres.resolveDSN()
	if err != nil {
		fail("postgres: %w", err)
	} else {
		cfg.Postgres.DSN = dsn
	}

	amqpURL, err := cfg.AMQP.resolveURL()
	if err != nil {
		fail("amqp: %w", err)
	} else {
		cfg.AMQP.URL = amqpURL
	}

	if cfg.Postgres.MaxConns < 1 {
		fail("%sPOSTGRES_MAX_CONNS must be at least 1", EnvPrefix)
	}
	if !strings.HasPrefix(cfg.HTTP.WebhookPath, "/") {
		fail("%sWEBHOOK_PATH must start with '/', got %q", EnvPrefix, cfg.HTTP.WebhookPath)
	}
	if cfg.HTTP.MaxBodyBytes < 1024 {
		fail("%sMAX_BODY_BYTES must be at least 1024", EnvPrefix)
	}
	// The publish budget has to fit inside the write timeout, or the handler is
	// still waiting for a confirm when the HTTP server has already given up on
	// the response.
	if cfg.AMQP.PublishTimeout >= cfg.HTTP.WriteTimeout {
		fail("%sHTTP_WRITE_TIMEOUT (%s) must exceed the fixed AMQP publish timeout (%s)",
			EnvPrefix, cfg.HTTP.WriteTimeout, cfg.AMQP.PublishTimeout)
	}
	if cfg.Log.Format != "json" && cfg.Log.Format != "text" {
		fail("%sLOG_FORMAT: %q is not one of \"json\", \"text\"", EnvPrefix, cfg.Log.Format)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

// resolveDSN merges the discrete POSTGRES_* variables over the DSN.
//
// The merge exists so that a Kubernetes Secret can hold only the password while
// the rest of the connection string stays readable in a ConfigMap. Supplying
// both a full DSN and discrete overrides is normal, not an error.
func (p PostgresConfig) resolveDSN() (string, error) {
	if p.DSN == "" && p.Host == "" {
		return "", fmt.Errorf("set %sPOSTGRES_DSN, or %sPOSTGRES_HOST and the other POSTGRES_* variables",
			EnvPrefix, EnvPrefix)
	}

	var u *url.URL
	if p.DSN != "" {
		parsed, err := url.Parse(p.DSN)
		if err != nil {
			// Deliberately does not include the DSN: it contains the password,
			// and a config error is the most likely thing to end up in a log
			// aggregator and a screenshot.
			return "", fmt.Errorf("%sPOSTGRES_DSN is not a valid URL: %w", EnvPrefix, err)
		}
		if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
			return "", fmt.Errorf("%sPOSTGRES_DSN scheme must be postgres:// or postgresql://, got %q",
				EnvPrefix, parsed.Scheme)
		}
		u = parsed
	} else {
		u = &url.URL{Scheme: "postgres", Host: "placeholder"}
	}

	user := u.User.Username()
	pass, _ := u.User.Password()
	if p.User != "" {
		user = p.User
	}
	if p.Password != "" {
		pass = p.Password
	}
	switch {
	case user != "" && pass != "":
		u.User = url.UserPassword(user, pass)
	case user != "":
		u.User = url.User(user)
	default:
		u.User = nil
	}

	host, port := splitHostPort(u.Host)
	if p.Host != "" {
		host = p.Host
	}
	if p.Port != 0 {
		port = strconv.Itoa(p.Port)
	}
	if host == "" || host == "placeholder" {
		return "", fmt.Errorf("%sPOSTGRES_DSN has no host and %sPOSTGRES_HOST is unset",
			EnvPrefix, EnvPrefix)
	}
	if port == "" {
		port = "5432"
	}
	u.Host = host + ":" + port

	if p.Database != "" {
		u.Path = "/" + strings.TrimPrefix(p.Database, "/")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/postgres"
	}

	q := u.Query()
	if p.SSLMode != "" {
		q.Set("sslmode", p.SSLMode)
	}
	// Supabase refuses plaintext connections, and pgx's own default is
	// `prefer` -- which silently falls back to plaintext against a server that
	// would have accepted TLS. Defaulting to require means a misconfiguration
	// fails loudly instead of connecting unencrypted.
	if q.Get("sslmode") == "" {
		q.Set("sslmode", "require")
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// resolveURL builds the AMQP URL from the discrete connection settings,
// escaping credentials supplied by the cluster operator's Secret.
func (a AMQPConfig) resolveURL() (string, error) {
	if a.Host == "" {
		return "", fmt.Errorf("%sAMQP_HOST must be set", EnvPrefix)
	}
	port := a.Port
	if port == 0 {
		port = 5672
	}
	u := &url.URL{
		Scheme: "amqp",
		Host:   a.Host + ":" + strconv.Itoa(port),
	}
	if a.Username != "" {
		u.User = url.UserPassword(a.Username, a.Password)
	}
	return u.String(), nil
}

// Redacted returns the DSN with its password replaced, for logging.
func Redacted(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<unparseable>"
	}
	if u.User != nil {
		if _, hasPass := u.User.Password(); hasPass {
			u.User = url.UserPassword(u.User.Username(), "xxxxx")
		}
	}
	return u.String()
}

func splitHostPort(hostport string) (host, port string) {
	if hostport == "" {
		return "", ""
	}
	// Not net.SplitHostPort: it errors when there is no port, which is the
	// common case here rather than an exceptional one.
	if i := strings.LastIndex(hostport, ":"); i >= 0 && !strings.Contains(hostport[i:], "]") {
		return hostport[:i], hostport[i+1:]
	}
	return hostport, ""
}

func env(name, def string) string {
	if v, ok := os.LookupEnv(EnvPrefix + name); ok && v != "" {
		return v
	}
	return def
}

func envInt(name string, def int, errs *[]error) int {
	raw := env(name, "")
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s%s: %q is not an integer", EnvPrefix, name, raw))
		return def
	}
	return v
}

func envInt64(name string, def int64, errs *[]error) int64 {
	raw := env(name, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s%s: %q is not an integer", EnvPrefix, name, raw))
		return def
	}
	return v
}

func envBool(name string, def bool, errs *[]error) bool {
	raw := env(name, "")
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s%s: %q is not a boolean (try true/false)", EnvPrefix, name, raw))
		return def
	}
	return v
}

func envDuration(name string, def time.Duration, errs *[]error) time.Duration {
	raw := env(name, "")
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s%s: %q is not a duration (try 5s, 200ms)", EnvPrefix, name, raw))
		return def
	}
	if v <= 0 {
		*errs = append(*errs, fmt.Errorf("%s%s must be positive, got %s", EnvPrefix, name, v))
		return def
	}
	return v
}
