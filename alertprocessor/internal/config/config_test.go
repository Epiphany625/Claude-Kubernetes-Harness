package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/Epiphany625/Claude-Kubernetes-Harness/alertprocessor/internal/config"
)

// setenv applies a set of ALERTPROCESSOR_* variables for the duration of a test.
// t.Setenv restores them and refuses to run under t.Parallel, which is what
// keeps these cases from leaking into each other.
func setenv(t *testing.T, vars map[string]string) {
	t.Helper()
	for k, v := range vars {
		t.Setenv(config.EnvPrefix+k, v)
	}
}

// minimal is the smallest configuration that loads.
func minimal() map[string]string {
	return map[string]string{
		"POSTGRES_DSN": "postgres://user:pass@db.example.com:5432/postgres?sslmode=require",
		"AMQP_HOST":    "rabbit",
	}
}

func TestLoadDefaults(t *testing.T) {
	setenv(t, minimal())

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q", cfg.HTTP.Addr)
	}
	// The webhook path is half of the contract with
	// ops/alerting/alertmanagerconfig.yaml; the default has to match what that
	// file POSTs to.
	if cfg.HTTP.WebhookPath != "/api/v1/alerts" {
		t.Errorf("HTTP.WebhookPath = %q", cfg.HTTP.WebhookPath)
	}
	if cfg.AMQP.Exchange != "alerts" || cfg.AMQP.Queue != "agent.events" {
		t.Errorf("AMQP topology defaults = %q / %q", cfg.AMQP.Exchange, cfg.AMQP.Queue)
	}
	if !cfg.Postgres.AutoMigrate {
		t.Error("DB_AUTO_MIGRATE should default to true so a fresh database works")
	}
}

func TestLoadReportsEveryProblemAtOnce(t *testing.T) {
	vars := minimal()
	vars["LOG_FORMAT"] = "xml"
	vars["WEBHOOK_PATH"] = "no-leading-slash"
	setenv(t, vars)

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected an error")
	}
	// A misconfigured deployment usually has more than one wrong variable, and
	// fixing them one CrashLoopBackOff at a time is miserable.
	for _, want := range []string{"LOG_FORMAT", "WEBHOOK_PATH"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestMissingConnectionDetails(t *testing.T) {
	t.Run("no postgres at all", func(t *testing.T) {
		setenv(t, map[string]string{"AMQP_HOST": "rabbit"})
		_, err := config.Load()
		if err == nil || !strings.Contains(err.Error(), "POSTGRES_DSN") {
			t.Fatalf("expected a POSTGRES_DSN error, got %v", err)
		}
	})

	t.Run("no amqp at all", func(t *testing.T) {
		setenv(t, map[string]string{"POSTGRES_DSN": "postgres://db:5432/postgres"})
		_, err := config.Load()
		if err == nil || !strings.Contains(err.Error(), "AMQP_HOST") {
			t.Fatalf("expected an AMQP_HOST error, got %v", err)
		}
	})
}

// The whole point of the discrete overrides: a Kubernetes Secret supplies only
// the password while the rest of the connection string stays readable in a
// ConfigMap, and nothing has to string-build a URL in a manifest.
func TestPostgresDiscreteOverrides(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
		want string
	}{
		{
			name: "password only",
			vars: map[string]string{
				"POSTGRES_DSN":      "postgres://postgres@db.example.com:5432/postgres?sslmode=require",
				"POSTGRES_PASSWORD": "hunter2",
			},
			want: "postgres://postgres:hunter2@db.example.com:5432/postgres?sslmode=require",
		},
		{
			name: "host and port",
			vars: map[string]string{
				"POSTGRES_DSN":  "postgres://u:p@old:5432/postgres?sslmode=require",
				"POSTGRES_HOST": "pooler.supabase.com",
				"POSTGRES_PORT": "6543",
			},
			want: "postgres://u:p@pooler.supabase.com:6543/postgres?sslmode=require",
		},
		{
			name: "no DSN at all, discrete only",
			vars: map[string]string{
				"POSTGRES_HOST":     "db.example.com",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "hunter2",
				"POSTGRES_DATABASE": "events",
			},
			want: "postgres://postgres:hunter2@db.example.com:5432/events?sslmode=require",
		},
		{
			// A password with URL-structural characters is exactly what the
			// merge exists to handle; hand-built URLs get this wrong.
			name: "password needing escaping",
			vars: map[string]string{
				"POSTGRES_HOST":     "db.example.com",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "p@ss:w/rd?&#",
			},
			want: "postgres://postgres:p%40ss%3Aw%2Frd%3F&%23@db.example.com:5432/postgres?sslmode=require",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vars := tc.vars
			vars["AMQP_HOST"] = "rabbit"
			setenv(t, vars)

			cfg, err := config.Load()
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Postgres.DSN != tc.want {
				t.Errorf("DSN =\n  %q\nwant\n  %q", cfg.Postgres.DSN, tc.want)
			}
		})
	}
}

// pgx's own default is sslmode=prefer, which silently falls back to plaintext.
// Supabase refuses plaintext, so defaulting to require turns a silent downgrade
// into a loud connection error.
func TestPostgresDefaultsToRequireTLS(t *testing.T) {
	vars := minimal()
	vars["POSTGRES_DSN"] = "postgres://u:p@db.example.com:5432/postgres"
	setenv(t, vars)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(cfg.Postgres.DSN, "sslmode=require") {
		t.Errorf("DSN %q did not gain sslmode=require", cfg.Postgres.DSN)
	}
}

func TestPostgresExplicitSSLModeWins(t *testing.T) {
	vars := minimal()
	vars["POSTGRES_DSN"] = "postgres://u:p@localhost:5432/postgres?sslmode=disable"
	setenv(t, vars)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Local development against a plaintext container has to remain possible.
	if !strings.Contains(cfg.Postgres.DSN, "sslmode=disable") {
		t.Errorf("DSN %q overrode an explicit sslmode", cfg.Postgres.DSN)
	}
}

func TestAMQPDiscreteOverrides(t *testing.T) {
	setenv(t, map[string]string{
		"POSTGRES_DSN": "postgres://u:p@db:5432/postgres?sslmode=require",
		"AMQP_HOST":    "agent-rabbitmq.ckh.svc.cluster.local",
		"AMQP_PORT":    "5672",
		// The RabbitMQ cluster operator generates passwords containing
		// characters that are structural in a URL. This is the case the discrete
		// variables exist for.
		"AMQP_USERNAME": "default_user_abc",
		"AMQP_PASSWORD": "p/w+d=x&y",
	})

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := "amqp://default_user_abc:p%2Fw+d=x&y@agent-rabbitmq.ckh.svc.cluster.local:5672"
	if cfg.AMQP.URL != want {
		t.Errorf("AMQP URL =\n  %q\nwant\n  %q", cfg.AMQP.URL, want)
	}
}

func TestAMQPFixedSettingsIgnoreEnvironment(t *testing.T) {
	vars := minimal()
	vars["AMQP_URL"] = "https://ignored:1234/custom"
	vars["AMQP_VHOST"] = "harness"
	vars["AMQP_DECLARE_TOPOLOGY"] = "false"
	setenv(t, vars)

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.AMQP.URL != "amqp://rabbit:5672" || cfg.AMQP.VHost != "/" || !cfg.AMQP.DeclareTopology {
		t.Fatal("AMQP environment overrides changed fixed settings")
	}
}

func TestInvalidURLs(t *testing.T) {
	t.Run("postgres wrong scheme", func(t *testing.T) {
		vars := minimal()
		vars["POSTGRES_DSN"] = "mysql://u:p@db:3306/x"
		setenv(t, vars)

		_, err := config.Load()
		if err == nil || !strings.Contains(err.Error(), "scheme") {
			t.Fatalf("expected a scheme error, got %v", err)
		}
		// The DSN holds a password, and a config error is the single most
		// likely thing to end up in a log aggregator and a screenshot.
		if strings.Contains(err.Error(), "p@db") {
			t.Errorf("error leaked DSN credentials: %v", err)
		}
	})

}

func TestDurationAndNumberParsing(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		vars := minimal()
		vars["POSTGRES_QUERY_TIMEOUT"] = "250ms"
		vars["POSTGRES_MAX_CONNS"] = "42"
		setenv(t, vars)

		cfg, err := config.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if cfg.Postgres.QueryTimeout != 250*time.Millisecond {
			t.Errorf("QueryTimeout = %v", cfg.Postgres.QueryTimeout)
		}
		if cfg.Postgres.MaxConns != 42 {
			t.Errorf("MaxConns = %d", cfg.Postgres.MaxConns)
		}
	})

	t.Run("a bare number is not a duration", func(t *testing.T) {
		vars := minimal()
		// "5" is the natural typo for "5s" and means nothing to ParseDuration.
		vars["POSTGRES_QUERY_TIMEOUT"] = "5"
		setenv(t, vars)

		_, err := config.Load()
		if err == nil || !strings.Contains(err.Error(), "duration") {
			t.Fatalf("expected a duration error, got %v", err)
		}
	})

	t.Run("negative duration", func(t *testing.T) {
		vars := minimal()
		vars["POSTGRES_QUERY_TIMEOUT"] = "-5s"
		setenv(t, vars)

		if _, err := config.Load(); err == nil {
			t.Fatal("expected a negative duration to be rejected")
		}
	})

	t.Run("non-numeric max conns", func(t *testing.T) {
		vars := minimal()
		vars["POSTGRES_MAX_CONNS"] = "lots"
		setenv(t, vars)

		if _, err := config.Load(); err == nil {
			t.Fatal("expected a non-numeric MAX_CONNS to be rejected")
		}
	})

	t.Run("zero max conns", func(t *testing.T) {
		vars := minimal()
		vars["POSTGRES_MAX_CONNS"] = "0"
		setenv(t, vars)

		if _, err := config.Load(); err == nil {
			t.Fatal("expected MAX_CONNS=0 to be rejected; a pool with no connections deadlocks")
		}
	})
}

// If a publish can still be waiting for a confirm after the HTTP server has
// given up on the response, the handler writes into a dead connection and
// Alertmanager sees a timeout rather than the 500 that would make it retry
// sensibly.
func TestPublishTimeoutMustFitInsideWriteTimeout(t *testing.T) {
	vars := minimal()
	vars["HTTP_WRITE_TIMEOUT"] = "5s"
	setenv(t, vars)

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected the publish timeout to be rejected for exceeding the write timeout")
	}
	if !strings.Contains(err.Error(), "HTTP_WRITE_TIMEOUT") {
		t.Errorf("error does not name the offending variable: %v", err)
	}
}

func TestRedacted(t *testing.T) {
	cases := []struct{ in, want string }{
		{"postgres://user:hunter2@db.example.com:5432/postgres", "postgres://user:xxxxx@db.example.com:5432/postgres"},
		{"amqp://default_user:s3cret@rabbit:5672/", "amqp://default_user:xxxxx@rabbit:5672/"},
		{"postgres://db.example.com:5432/postgres", "postgres://db.example.com:5432/postgres"},
		{"://nonsense", "<unparseable>"},
	}
	for _, tc := range cases {
		got := config.Redacted(tc.in)
		if got != tc.want {
			t.Errorf("Redacted(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.Contains(got, "hunter2") || strings.Contains(got, "s3cret") {
			t.Errorf("Redacted(%q) leaked the password: %q", tc.in, got)
		}
	}
}
