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

// PoolMode describes how the Postgres endpoint we connect to pools connections.
// It is not cosmetic: it decides whether pgx may use prepared statements.
type PoolMode string

const (
	// PoolModeSession is a direct connection (Supabase port 5432, or any plain
	// Postgres). Each connection is ours for its lifetime, so the prepared
	// statement cache is valid and worth having.
	PoolModeSession PoolMode = "session"

	// PoolModeTransaction is a transaction-mode pooler in front of Postgres
	// (Supabase's pooler on port 6543, pgbouncer generally). The backend
	// connection changes between transactions, so a statement prepared on one
	// is not there on the next. pgx must send queries unprepared.
	PoolModeTransaction PoolMode = "transaction"
)

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
	// WebhookToken is the shared bearer token Alertmanager presents. Empty
	// disables authentication -- a deliberate escape hatch for first-run and
	// local testing, logged loudly at startup so it is not an accident that
	// survives to a shared cluster.
	WebhookToken string
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
	PoolMode       PoolMode
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
			WebhookToken:    env("WEBHOOK_TOKEN", ""),
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
			PoolMode:       PoolMode(strings.ToLower(env("POSTGRES_POOL_MODE", string(PoolModeSession)))),
			MaxConns:       int32(envInt("POSTGRES_MAX_CONNS", 10, &errs)),
			MinConns:       int32(envInt("POSTGRES_MIN_CONNS", 0, &errs)),
			ConnectTimeout: envDuration("POSTGRES_CONNECT_TIMEOUT", 10*time.Second, &errs),
			QueryTimeout:   envDuration("POSTGRES_QUERY_TIMEOUT", 5*time.Second, &errs),
			AutoMigrate:    envBool("DB_AUTO_MIGRATE", true, &errs),
		},
		AMQP: AMQPConfig{
			URL:      env("AMQP_URL", ""),
			Host:     env("AMQP_HOST", ""),
			Port:     envInt("AMQP_PORT", 0, &errs),
			Username: env("AMQP_USERNAME", ""),
			Password: env("AMQP_PASSWORD", ""),
			VHost:    env("AMQP_VHOST", "/"),
			// Trimmed, because a name with surrounding whitespace is almost
			// always a YAML quoting accident, and RabbitMQ would accept
			// "agent.events " as a genuinely different queue from the one the
			// consumer binds to.
			Exchange:        strings.TrimSpace(env("AMQP_EXCHANGE", "alerts")),
			Queue:           strings.TrimSpace(env("AMQP_QUEUE", "agent.events")),
			RoutingPrefix:   strings.TrimSpace(env("AMQP_ROUTING_KEY_PREFIX", "alert")),
			DeclareTopology: envBool("AMQP_DECLARE_TOPOLOGY", true, &errs),
			ConfirmTimeout:  envDuration("AMQP_CONFIRM_TIMEOUT", 5*time.Second, &errs),
			ConnectTimeout:  envDuration("AMQP_CONNECT_TIMEOUT", 10*time.Second, &errs),
			PublishTimeout:  envDuration("AMQP_PUBLISH_TIMEOUT", 8*time.Second, &errs),
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

	if cfg.Postgres.PoolMode != PoolModeSession && cfg.Postgres.PoolMode != PoolModeTransaction {
		fail("%sPOSTGRES_POOL_MODE: %q is not one of %q, %q",
			EnvPrefix, cfg.Postgres.PoolMode, PoolModeSession, PoolModeTransaction)
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
	if cfg.AMQP.Exchange == "" {
		fail("%sAMQP_EXCHANGE must not be empty", EnvPrefix)
	}
	if cfg.AMQP.DeclareTopology && cfg.AMQP.Queue == "" {
		fail("%sAMQP_QUEUE must be set when %sAMQP_DECLARE_TOPOLOGY is true",
			EnvPrefix, EnvPrefix)
	}
	// The publish budget has to fit inside the write timeout, or the handler is
	// still waiting for a confirm when the HTTP server has already given up on
	// the response.
	if cfg.AMQP.PublishTimeout >= cfg.HTTP.WriteTimeout {
		fail("%sAMQP_PUBLISH_TIMEOUT (%s) must be less than %sHTTP_WRITE_TIMEOUT (%s)",
			EnvPrefix, cfg.AMQP.PublishTimeout, EnvPrefix, cfg.HTTP.WriteTimeout)
	}
	if cfg.Log.Format != "json" && cfg.Log.Format != "text" {
		fail("%sLOG_FORMAT: %q is not one of \"json\", \"text\"", EnvPrefix, cfg.Log.Format)
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cfg, nil
}

// AuthEnabled reports whether the webhook checks a bearer token.
func (c HTTPConfig) AuthEnabled() bool { return c.WebhookToken != "" }

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

// resolveURL builds the AMQP URL the same way, so RabbitMQ credentials can come
// straight from the cluster operator's Secret without anyone string-building a
// URL in a manifest (and URL-escaping the password by hand).
func (a AMQPConfig) resolveURL() (string, error) {
	if a.URL == "" && a.Host == "" {
		return "", fmt.Errorf("set %sAMQP_URL, or %sAMQP_HOST and the other AMQP_* variables",
			EnvPrefix, EnvPrefix)
	}

	var u *url.URL
	if a.URL != "" {
		parsed, err := url.Parse(a.URL)
		if err != nil {
			return "", fmt.Errorf("%sAMQP_URL is not a valid URL: %w", EnvPrefix, err)
		}
		if parsed.Scheme != "amqp" && parsed.Scheme != "amqps" {
			return "", fmt.Errorf("%sAMQP_URL scheme must be amqp:// or amqps://, got %q",
				EnvPrefix, parsed.Scheme)
		}
		u = parsed
	} else {
		u = &url.URL{Scheme: "amqp"}
	}

	user := u.User.Username()
	pass, _ := u.User.Password()
	if a.Username != "" {
		user = a.Username
	}
	if a.Password != "" {
		pass = a.Password
	}
	if user != "" {
		// url.UserPassword escapes both halves. RabbitMQ's generated passwords
		// routinely contain characters that are structural in a URL, which is
		// the bug this whole function exists to prevent.
		u.User = url.UserPassword(user, pass)
	}

	host, port := splitHostPort(u.Host)
	if a.Host != "" {
		host = a.Host
	}
	if a.Port != 0 {
		port = strconv.Itoa(a.Port)
	}
	if host == "" {
		return "", fmt.Errorf("%sAMQP_URL has no host and %sAMQP_HOST is unset", EnvPrefix, EnvPrefix)
	}
	if port == "" {
		port = "5672"
	}
	u.Host = host + ":" + port

	if a.VHost != "" && a.VHost != "/" {
		u.Path = "/" + strings.TrimPrefix(a.VHost, "/")
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
