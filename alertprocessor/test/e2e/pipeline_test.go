//go:build e2e

// End-to-end test of the deployed pipeline.
//
//	make test-e2e     # or: go test -tags=e2e ./test/e2e/...
//
// This is the only test that proves the ops/ configuration is right. Everything
// else stubs out the part where Alertmanager decides whether to call us at all,
// and that decision -- the matcher strategy, the label selector, the sub-route
// ordering -- is where this pipeline actually breaks.
//
// It injects a synthetic alert into the *Alertmanager API*, not into the
// service's webhook. Posting straight at the webhook would pass with the routing
// completely misconfigured, which would make this test worse than useless.
//
// Prerequisites (all checked with a clear message before anything runs):
//   - a cluster with the pipeline deployed, reachable via the current kubectl context
//   - ALERTPROCESSOR_POSTGRES_DSN pointing at the same database the service uses
//
// Port-forwards to Alertmanager and the service are started and torn down here.
package e2e

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	namespace           = "ckh"
	monitoringNamespace = "monitoring"
	alertmanagerService = "svc/monitoring-kube-prometheus-alertmanager"
	processorService    = "svc/alertprocessor"
)

// ---------------------------------------------------------------------------
// Prerequisites
// ---------------------------------------------------------------------------

func requireKubectl(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl is not on PATH; skipping the live-cluster suite")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", "get", "deploy", "alertprocessor",
		"-n", namespace, "-o", "jsonpath={.status.readyReplicas}").CombinedOutput()
	if err != nil {
		t.Skipf("alertprocessor is not deployed in %s (%v: %s); "+
			"apply ops/alertprocessor/ first", namespace, err, out)
	}
	if string(out) == "" || string(out) == "0" {
		t.Fatalf("alertprocessor has no ready replicas; `kubectl -n %s get pods` to see why", namespace)
	}
}

func requireDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ALERTPROCESSOR_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ALERTPROCESSOR_POSTGRES_DSN is unset; " +
			"this test has to read the same database the deployed service writes to")
	}
	return dsn
}

// ---------------------------------------------------------------------------
// Port forwarding
// ---------------------------------------------------------------------------

// portForward starts `kubectl port-forward` on a free local port and waits until
// the port accepts connections. The forward is killed by t.Cleanup.
func portForward(t *testing.T, ns, resource string, remotePort int) int {
	t.Helper()

	local := freePort(t)
	cmd := exec.Command("kubectl", "port-forward", "-n", ns, resource,
		fmt.Sprintf("%d:%d", local, remotePort))
	// Inherit stderr so a failure to forward is visible rather than a mystery
	// timeout below.
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start port-forward to %s/%s: %v", ns, resource, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", local), time.Second)
		if err == nil {
			_ = conn.Close()
			return local
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("port-forward to %s/%s did not become ready within 30s", ns, resource)
	return 0
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// ---------------------------------------------------------------------------
// Alertmanager
// ---------------------------------------------------------------------------

type apiAlert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt,omitempty"`
	GeneratorURL string            `json:"generatorURL"`
}

// postAlert injects an alert into Alertmanager's v2 API, exactly as Prometheus
// would.
func postAlert(t *testing.T, port int, alerts []apiAlert) {
	t.Helper()

	body, err := json.Marshal(alerts)
	if err != nil {
		t.Fatalf("marshal alerts: %v", err)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/api/v2/alerts", port)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("post to alertmanager: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		t.Fatalf("alertmanager answered %s to the injected alert", resp.Status)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// The whole chain: Alertmanager's routing decision, the webhook call, the
// insert, the publish.
func TestAlertReachesTheEventTable(t *testing.T) {
	requireKubectl(t)
	dsn := requireDSN(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	// t.Cleanup, not defer. Cleanups run LIFO and *after* every defer, so a
	// deferred Close would shut the pool before the row cleanup registered later
	// could use it -- leaving probe rows behind with only a log line to say so.
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	amPort := portForward(t, monitoringNamespace, alertmanagerService, 9093)

	// A unique alertname per run, so the assertion below cannot match a row left
	// behind by an earlier run or by a real alert that happens to be firing.
	alertName := "E2EPipelineProbe" + uuid.NewString()[:8]
	startsAt := time.Now().UTC().Truncate(time.Second)

	postAlert(t, amPort, []apiAlert{{
		Labels: map[string]string{
			"alertname": alertName,
			"namespace": namespace,
			"pod":       "e2e-probe-0",
			"severity":  "warning",
			"origin":    "alertprocessor-e2e",
		},
		Annotations: map[string]string{
			"summary":     "Synthetic alert injected by the alertprocessor e2e suite.",
			"description": "If this row is in the event table, Alertmanager routing works.",
		},
		StartsAt:     startsAt,
		GeneratorURL: "http://e2e.invalid/",
	}})

	// groupWait is 30s in ops/alerting/alertmanagerconfig.yaml, so nothing can
	// arrive sooner than that however healthy the pipeline is.
	t.Logf("injected %s; waiting for it to be grouped (groupWait 30s) and delivered", alertName)

	var (
		eventID     string
		alertID     string
		status      string
		alertStatus string
		publishedAt *time.Time
	)

	deadline := time.Now().Add(2 * time.Minute)
	for {
		err := pool.QueryRow(ctx, `
			SELECT event_id::text, alert_id, status, alert_status, published_at
			  FROM event
			 WHERE alert_name = $1
			 ORDER BY received_at DESC
			 LIMIT 1`, alertName).
			Scan(&eventID, &alertID, &status, &alertStatus, &publishedAt)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no event row for %s within 2 minutes.\n\n"+
				"Work backwards through the chain:\n"+
				"  1. Did Alertmanager route it? Read the generated config:\n"+
				"     kubectl -n monitoring get secret "+
				"alertmanager-monitoring-kube-prometheus-alertmanager-generated "+
				"-o jsonpath='{.data.alertmanager\\.yaml}' | base64 -d\n"+
				"     If the only receiver is \"null\", the patch or the "+
				"AlertmanagerConfig did not take.\n"+
				"  2. Did the webhook arrive? kubectl -n %s logs -l "+
				"app.kubernetes.io/name=alertprocessor --tail=50\n"+
				"  3. Last error: %v", alertName, namespace, err)
		}
		time.Sleep(3 * time.Second)
	}

	t.Logf("event %s recorded for alert %s", eventID, alertID)

	// The requirement this service exists to satisfy.
	if status != "waiting" {
		t.Errorf("status = %q, want waiting -- this service writes no other status", status)
	}
	if alertStatus != "firing" {
		t.Errorf("alert_status = %q, want firing", alertStatus)
	}
	if alertID == "" {
		t.Error("alert_id is empty")
	}
	if eventID == alertID {
		t.Error("event_id and alert_id are the same value; they are different identities")
	}
	// published_at is set only after RabbitMQ confirms, so this asserts the
	// handoff really happened rather than just the insert.
	if publishedAt == nil {
		t.Error("published_at is NULL: the row was recorded but never reached RabbitMQ. " +
			"Check the exchange has a queue bound to it -- an unroutable publish is " +
			"the usual cause.")
	}

	// Leave nothing behind; a stale probe row would confuse the next run's
	// operator, if not its assertions.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM event WHERE alert_name = $1`, alertName); err != nil {
			t.Logf("clean up probe rows for %s: %v", alertName, err)
		}
	})
}

// The service's own surface, through the Service DNS name rather than a pod IP.
func TestProbesAndMetrics(t *testing.T) {
	requireKubectl(t)

	port := portForward(t, namespace, processorService, 8080)
	client := &http.Client{Timeout: 10 * time.Second}

	for _, tc := range []struct {
		path string
		want int
	}{
		{"/healthz", http.StatusOK},
		{"/readyz", http.StatusOK},
		{"/metrics", http.StatusOK},
	} {
		t.Run(tc.path, func(t *testing.T) {
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, tc.path))
			if err != nil {
				t.Fatalf("GET %s: %v", tc.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Errorf("GET %s = %s, want %d", tc.path, resp.Status, tc.want)
			}
		})
	}
}

// generatedAlertmanagerConfig returns the configuration the operator actually
// produced -- which is a different object from the one Helm wrote, and the only
// one Alertmanager runs.
//
// prometheus-operator >= 0.78 stores it gzipped under `alertmanager.yaml.gz`;
// older versions use a plain `alertmanager.yaml`. Both are handled, because
// reading the wrong key yields an empty string rather than an error, and the
// resulting test failure points at the routing instead of at the lookup.
func generatedAlertmanagerConfig(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, "kubectl", "-n", monitoringNamespace,
		"get", "secret",
		"alertmanager-monitoring-kube-prometheus-alertmanager-generated",
		"-o", "json").Output()
	if err != nil {
		t.Fatalf("read the generated alertmanager config secret: %v", err)
	}

	var secret struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(out, &secret); err != nil {
		t.Fatalf("decode secret: %v", err)
	}

	if encoded, ok := secret.Data["alertmanager.yaml.gz"]; ok && encoded != "" {
		gz, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatalf("base64-decode alertmanager.yaml.gz: %v", err)
		}
		zr, err := gzip.NewReader(bytes.NewReader(gz))
		if err != nil {
			t.Fatalf("gunzip alertmanager.yaml.gz: %v", err)
		}
		defer zr.Close()
		plain, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read alertmanager.yaml.gz: %v", err)
		}
		return string(plain)
	}

	encoded, ok := secret.Data["alertmanager.yaml"]
	if !ok || encoded == "" {
		t.Fatalf("the generated secret has neither alertmanager.yaml.gz nor "+
			"alertmanager.yaml; keys present: %v", keysOf(secret.Data))
	}
	plain, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64-decode alertmanager.yaml: %v", err)
	}
	return string(plain)
}

func keysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Alertmanager must be configured to send somewhere other than "null". This
// fails fast and specifically, where TestAlertReachesTheEventTable would fail
// after two minutes of waiting.
func TestAlertmanagerRoutesToTheProcessor(t *testing.T) {
	requireKubectl(t)

	config := generatedAlertmanagerConfig(t)

	// The operator names a merged receiver <namespace>/<config>/<receiver>, so a
	// bare "alertprocessor" is not what appears in the file.
	if !strings.Contains(config, "ckh/alertprocessor/alertprocessor") {
		t.Fatalf("the generated Alertmanager config has no alertprocessor receiver.\n"+
			"Apply ops/alerting/alertmanager-patch.yaml and ops/alerting/alertmanagerconfig.yaml,\n"+
			"in that order.\nCurrent config:\n%s", config)
	}
	if !strings.Contains(config, "alertprocessor.ckh.svc.cluster.local:8080") {
		t.Errorf("the receiver does not point at the alertprocessor Service.\n%s", config)
	}

	// The operator injects this when alertmanagerConfigMatcherStrategy is left at
	// its default, and it silently limits the pipeline to alerts about ckh --
	// which a smoke test using a ckh-labelled alert would not catch.
	if strings.Contains(config, `namespace="ckh"`) || strings.Contains(config, "namespace = ckh") {
		t.Errorf("the merged route carries an injected namespace matcher, so alerts " +
			"from other namespaces will never reach the processor. Apply " +
			"ops/alerting/alertmanager-patch.yaml (alertmanagerConfigMatcherStrategy: None).")
	}

	// Merged routes are prepended and forced to continue:true, so the chart's
	// own `Watchdog -> "null"` route does not shield us. Without this matcher the
	// event table fills with a deadman's switch that can never be resolved.
	if !strings.Contains(config, "Watchdog|InfoInhibitor") {
		t.Errorf("the merged route does not exclude the always-firing synthetic " +
			"alerts. Check the `matchers` block in ops/alerting/alertmanagerconfig.yaml.")
	}
}
