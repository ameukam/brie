// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package hw_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const (
	testProject = "brie-lab-test"
	testRunID   = "r20261113"
	testPrefix  = "brie-hw-r20261113"
	testMarker  = "brie-hw-run=r20261113"
	testZone    = "us-east4-c"
	testZone2   = "us-east4-a"
	testRegion  = "us-east4"
	// testClockStart is 2026-11-13T12:00:00Z.
	testClockStart = 1794571200
)

func TestMain(m *testing.M) {
	if tool := os.Getenv(mockToolEnv); tool != "" {
		os.Exit(runMock(tool, os.Args[1:]))
	}
	os.Exit(m.Run())
}

type lab struct {
	t   *testing.T
	dir string
	env map[string]string
}

type result struct {
	stdout, stderr string
	code           int
}

// newLab prepares an isolated directory whose PATH starts with the mock
// gcloud, date, and sleep. CLOUDSDK_CONFIG and HOME point at empty
// directories, so even an accidental real gcloud would have no credentials.
func newLab(t *testing.T, state mockState) *lab {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	for _, d := range []string{bin, filepath.Join(dir, "home"), filepath.Join(dir, "gcloud-config")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	realDate, err := exec.LookPath("date")
	if err != nil {
		t.Fatal(err)
	}
	wrappers := map[string]string{
		// A race-enabled test binary otherwise sleeps one second at exit.
		"gcloud": "#!/bin/sh\nGORACE=atexit_sleep_ms=0 BRIE_HW_MOCK_TOOL=gcloud exec '" + self + "' \"$@\"\n",
		"date": "#!/bin/sh\nnow=$(cat \"$BRIE_HW_MOCK_CLOCK\")\nformat=\nfor arg; do case $arg in +*) format=$arg ;; esac; done\n" +
			"case $format in\n+%s) echo \"$now\" ;;\n+*) exec '" + realDate + "' -u -d \"@$now\" \"$format\" ;;\n" +
			"*) echo \"mock date: unsupported $*\" >&2; exit 2 ;;\nesac\n",
		"kubectl": "#!/bin/sh\nGORACE=atexit_sleep_ms=0 BRIE_HW_MOCK_TOOL=kubectl exec '" + self + "' \"$@\"\n",
		"sleep":   "#!/bin/sh\nnow=$(cat \"$BRIE_HW_MOCK_CLOCK\")\necho $((now + $1)) >\"$BRIE_HW_MOCK_CLOCK\"\n",
	}
	for name, body := range wrappers {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if state.Project == "" {
		state.Project = testProject
	}
	l := &lab{t: t, dir: dir, env: map[string]string{
		"PATH":                  bin + ":/usr/local/bin:/usr/bin:/bin",
		"HOME":                  filepath.Join(dir, "home"),
		"CLOUDSDK_CONFIG":       filepath.Join(dir, "gcloud-config"),
		mockStateEnv:            filepath.Join(dir, "state.json"),
		mockLogEnv:              filepath.Join(dir, "calls.jsonl"),
		mockClockEnv:            filepath.Join(dir, "clock"),
		"APPROVE_COSTS":         "yes",
		"PROJECT":               testProject,
		"ZONE":                  testZone,
		"ZONE2":                 testZone2,
		"RUN_ID":                testRunID,
		"SPEND_CEILING_USD":     "500",
		"HW_LOG_DIR":            filepath.Join(dir, "log"),
		"CLOUDSDK_CORE_PROJECT": "",
	}}
	l.setState(state)
	if err := os.WriteFile(l.env[mockClockEnv], []byte(fmt.Sprintf("%d\n", testClockStart)), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"gcloud", "kubectl"} {
		if r := l.runCommand("sh", "-c", "command -v "+tool); strings.TrimSpace(r.stdout) != filepath.Join(bin, tool) {
			t.Fatalf("%s resolves to %q, not the mock; refusing to run scripts", tool, r.stdout)
		}
	}
	return l
}

func (l *lab) setState(state mockState) {
	l.t.Helper()
	if state.Project == "" {
		state.Project = testProject
	}
	if err := saveMockState(l.env[mockStateEnv], &state); err != nil {
		l.t.Fatal(err)
	}
}

func (l *lab) state() mockState {
	l.t.Helper()
	state, err := loadMockState(l.env[mockStateEnv])
	if err != nil {
		l.t.Fatal(err)
	}
	return *state
}

func (l *lab) run(script string, args ...string) result {
	l.t.Helper()
	path, err := filepath.Abs(script)
	if err != nil {
		l.t.Fatal(err)
	}
	return l.runCommand("bash", append([]string{path}, args...)...)
}

func (l *lab) runCommand(name string, args ...string) result {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(l.t.Context(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = l.dir
	for k, v := range l.env {
		if v != "" {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			l.t.Fatalf("run %s: %v", name, err)
		}
		code = exitErr.ExitCode()
	}
	return result{stdout: stdout.String(), stderr: stderr.String(), code: code}
}

// calls returns the recorded mock invocations and checks the default-project
// guard: every gcloud call passes --project explicitly as its first argument
// and no call uses gcloud config.
func (l *lab) calls() []mockCall {
	l.t.Helper()
	f, err := os.Open(l.env[mockLogEnv])
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		l.t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var calls []mockCall
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var call mockCall
		if err := json.Unmarshal(scanner.Bytes(), &call); err != nil {
			l.t.Fatal(err)
		}
		if call.Tool == "gcloud" {
			if len(call.Args) == 0 || call.Args[0] != "--project="+testProject {
				l.t.Errorf("gcloud call without leading explicit project: %q", call.Args)
			}
			for _, arg := range call.Args {
				if arg == "config" {
					l.t.Errorf("gcloud config used: %q", call.Args)
				}
			}
		}
		calls = append(calls, call)
	}
	if err := scanner.Err(); err != nil {
		l.t.Fatal(err)
	}
	return calls
}

// commands renders the gcloud calls without the project flag, one per line.
func (l *lab) commands() []string {
	var out []string
	for _, call := range l.calls() {
		if call.Tool == "gcloud" && len(call.Args) > 0 {
			out = append(out, strings.Join(call.Args[1:], " "))
		}
	}
	return out
}

func indexOf(commands []string, pattern string) int {
	re := regexp.MustCompile(pattern)
	for i, c := range commands {
		if re.MatchString(c) {
			return i
		}
	}
	return -1
}

func runLabels() map[string]string { return map[string]string{"brie-hw-run": testRunID} }

// ownedLab returns one resource of every type the lab creates, all carrying
// this run's marker, plus an unrelated default network.
func ownedLab() []mockResource {
	return []mockResource{
		{Kind: "instances", Name: testPrefix + "-metal-1", Zone: testZone, Labels: runLabels(), Status: "RUNNING"},
		{Kind: "disks", Name: testPrefix + "-metal-1-data", Zone: testZone, Labels: runLabels()},
		{Kind: "buckets", Name: testPrefix + "-" + testProject, Labels: runLabels()},
		{Kind: "firewall-rules", Name: testPrefix + "-internal", Description: testMarker},
		{Kind: "routers", Name: testPrefix + "-router", Region: testRegion, Description: testMarker},
		{Kind: "nats", Name: testPrefix + "-nat", Router: testPrefix + "-router", Region: testRegion},
		{Kind: "subnets", Name: testPrefix + "-subnet", Region: testRegion, Description: testMarker},
		{Kind: "networks", Name: testPrefix + "-net", Description: testMarker},
		{Kind: "networks", Name: "default"},
		{Kind: "instances", Name: "unrelated-vm", Zone: testZone, Labels: map[string]string{"team": "x"}},
	}
}

var scripts = []string{"verify-empty.sh", "teardown.sh", "provision.sh", "fence.sh", "run-on-host.sh", "m1-harness.sh"}

func TestRequiredInputs(t *testing.T) {
	for _, script := range scripts {
		for _, tt := range []struct {
			name   string
			env    map[string]string
			stderr string
		}{
			{name: "no approval", env: map[string]string{"APPROVE_COSTS": ""}, stderr: "set APPROVE_COSTS=yes"},
			{name: "approval not yes", env: map[string]string{"APPROVE_COSTS": "YES"}, stderr: "set APPROVE_COSTS=yes"},
			{name: "missing project", env: map[string]string{"PROJECT": ""}, stderr: "required input PROJECT is not set"},
			{name: "missing zone", env: map[string]string{"ZONE": ""}, stderr: "required input ZONE is not set"},
			{name: "missing zone2", env: map[string]string{"ZONE2": ""}, stderr: "required input ZONE2 is not set"},
			{name: "missing run id", env: map[string]string{"RUN_ID": ""}, stderr: "required input RUN_ID is not set"},
			{name: "missing ceiling", env: map[string]string{"SPEND_CEILING_USD": ""}, stderr: "required input SPEND_CEILING_USD is not set"},
			{name: "uppercase run id", env: map[string]string{"RUN_ID": "R20261113"}, stderr: "RUN_ID R20261113 must match"},
			{name: "run id with separator", env: map[string]string{"RUN_ID": "r2026_1113"}, stderr: "must match"},
			{name: "run id too long", env: map[string]string{"RUN_ID": "r2026111300000000"}, stderr: "must match"},
			{name: "invalid project", env: map[string]string{"PROJECT": "Bad Project"}, stderr: "not a valid project ID"},
			{name: "same zones", env: map[string]string{"ZONE2": testZone}, stderr: "ZONE2 must differ"},
			{name: "cross-region fallback", env: map[string]string{"ZONE2": "us-central1-a"}, stderr: "same region"},
			{name: "zero ceiling", env: map[string]string{"SPEND_CEILING_USD": "0"}, stderr: "greater than zero"},
			{name: "non-numeric ceiling", env: map[string]string{"SPEND_CEILING_USD": "$100"}, stderr: "positive USD amount"},
		} {
			t.Run(script+"/"+tt.name, func(t *testing.T) {
				l := newLab(t, mockState{Resources: ownedLab()})
				for k, v := range tt.env {
					l.env[k] = v
				}
				r := l.run(script)
				if r.code == 0 || !strings.Contains(r.stderr, tt.stderr) {
					t.Fatalf("exit=%d stderr=%q; want failure containing %q", r.code, r.stderr, tt.stderr)
				}
				if calls := l.calls(); len(calls) != 0 {
					t.Fatalf("refused run made calls: %v", calls)
				}
			})
		}
	}
}

// TestDefaultProjectGuard checks that an ambient project setting is ignored
// and that no script source mentions gcloud config.
func TestDefaultProjectGuard(t *testing.T) {
	l := newLab(t, mockState{})
	l.env["CLOUDSDK_CORE_PROJECT"] = "kubernetes-public"
	if r := l.run("verify-empty.sh"); r.code != 0 {
		t.Fatalf("verify-empty: exit=%d stderr=%q", r.code, r.stderr)
	}
	if n := len(l.calls()); n != 7 {
		t.Fatalf("got %d gcloud calls, want 7", n)
	}

	files, err := filepath.Glob("*.sh")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := regexp.MustCompile(`\bgcloud\s+(--\S+\s+)*config\b|get-value|core/project`)
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if loc := forbidden.FindIndex(data); loc != nil {
			t.Errorf("%s reads gcloud configuration: %q", file, data[loc[0]:loc[1]])
		}
	}
}

func TestTeardown(t *testing.T) {
	foreign := map[string]string{"brie-hw-run": "r20261112"}
	for _, tt := range []struct {
		name     string
		extra    []mockResource
		modify   func([]mockResource)
		fail     []string
		messages []string
		code     int
		refused  []string
		retained []string
	}{
		{name: "all owned", code: 0},
		{
			name:     "instance label mismatch",
			extra:    []mockResource{{Kind: "instances", Name: testPrefix + "-metal-2", Zone: testZone2, Labels: foreign}},
			code:     1,
			refused:  []string{"instances " + testPrefix + "-metal-2 in " + testZone2},
			retained: []string{"instances/" + testPrefix + "-metal-2"},
		},
		{
			name:     "unlabelled disk",
			extra:    []mockResource{{Kind: "disks", Name: testPrefix + "-metal-2"}},
			code:     1,
			refused:  []string{"disks " + testPrefix + "-metal-2"},
			retained: []string{"disks/" + testPrefix + "-metal-2"},
		},
		{
			name:     "bucket label mismatch",
			modify:   func(rs []mockResource) { rs[2].Labels = foreign },
			code:     1,
			refused:  []string{"bucket gs://" + testPrefix},
			retained: []string{"buckets/" + testPrefix + "-" + testProject},
		},
		{
			name:     "network without marker",
			modify:   func(rs []mockResource) { rs[7].Description = "" },
			code:     1,
			refused:  []string{"network " + testPrefix + "-net"},
			retained: []string{"networks/" + testPrefix + "-net"},
		},
		{
			name:     "failed instance list",
			fail:     []string{"compute instances list"},
			messages: []string{"teardown: FAILED to list: compute instances list", "verify-empty: instances: list failed"},
			code:     1,
			retained: []string{"instances/" + testPrefix + "-metal-1"},
		},
		{
			name:     "failed disk delete",
			fail:     []string{"compute disks delete"},
			messages: []string{"teardown: FAILED compute disks delete " + testPrefix + "-metal-1-data"},
			code:     1,
			retained: []string{"disks/" + testPrefix + "-metal-1-data"},
		},
		{
			name:     "router with another marker keeps its NAT",
			modify:   func(rs []mockResource) { rs[4].Description = "brie-hw-run=r20261112" },
			code:     1,
			refused:  []string{"router " + testPrefix + "-router"},
			retained: []string{"routers/" + testPrefix + "-router", "nats/" + testPrefix + "-nat"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resources := ownedLab()
			if tt.modify != nil {
				tt.modify(resources)
			}
			l := newLab(t, mockState{Resources: append(resources, tt.extra...), Fail: tt.fail})
			r := l.run("teardown.sh")
			if r.code != tt.code {
				t.Fatalf("exit=%d, want %d; stdout=%q stderr=%q", r.code, tt.code, r.stdout, r.stderr)
			}
			for _, message := range tt.messages {
				if !strings.Contains(r.stderr, message) {
					t.Errorf("stderr=%q; want %q", r.stderr, message)
				}
			}
			for _, refused := range tt.refused {
				if !strings.Contains(r.stderr, "REFUSED "+refused) {
					t.Errorf("stderr=%q; want refusal of %q", r.stderr, refused)
				}
			}
			if tt.code != 0 && !strings.Contains(r.stderr, "verify-empty: FAILED") {
				t.Errorf("verify-empty did not fail after a refusal: %q", r.stderr)
			}
			state := l.state()
			want := map[string]bool{"networks/default": true, "instances/unrelated-vm": true}
			for _, kept := range tt.retained {
				want[kept] = true
			}
			for _, res := range state.Resources {
				if !want[res.Kind+"/"+res.Name] {
					t.Errorf("resource %s/%s survived teardown", res.Kind, res.Name)
				}
				delete(want, res.Kind+"/"+res.Name)
			}
			for missing := range want {
				t.Errorf("resource %s was deleted; it must be retained", missing)
			}

			commands := l.commands()
			for _, c := range commands {
				if strings.Contains(c, "default") || strings.Contains(c, "unrelated-vm") {
					t.Errorf("teardown touched an unrelated resource: %q", c)
				}
			}
			// Every delete is preceded by a describe of the same resource.
			for i, c := range commands {
				fields := strings.Fields(c)
				if len(fields) < 4 || (fields[2] != "delete" && fields[1] != "rm") {
					continue
				}
				if fields[1] == "routers" && fields[2] == "nats" {
					continue
				}
				name := fields[3]
				described := false
				for _, prior := range commands[:i] {
					if strings.Contains(prior, " describe "+name) {
						described = true
					}
				}
				if !described {
					t.Errorf("deleted without re-describing: %q", c)
				}
			}
			if tt.code == 0 {
				order := []string{
					`^compute instances delete`, `^compute disks delete`, `^storage rm --recursive gs://`,
					`^compute firewall-rules delete`, `^compute routers nats delete`, `^compute routers delete`,
					`^compute networks subnets delete`, `^compute networks delete`,
				}
				last := -1
				for _, pattern := range order {
					i := indexOf(commands, pattern)
					if i <= last {
						t.Errorf("%q at %d is out of dependency order (previous %d)", pattern, i, last)
					}
					last = i
				}
			}
		})
	}
}

func TestVerifyEmpty(t *testing.T) {
	for _, tt := range []struct {
		name      string
		resources []mockResource
		fail      []string
		code      int
		stderr    string
	}{
		{name: "empty", resources: []mockResource{{Kind: "networks", Name: "default"}}},
		{name: "bucket list fails", fail: []string{"storage buckets list"}, code: 1, stderr: "buckets: list failed; cannot verify"},
		{
			name:      "labelled disk remains",
			resources: []mockResource{{Kind: "disks", Name: "renamed-disk", Zone: testZone, Labels: runLabels()}},
			code:      1, stderr: "remaining disks: renamed-disk",
		},
		{
			name:      "prefixed firewall without marker remains",
			resources: []mockResource{{Kind: "firewall-rules", Name: testPrefix + "-ssh"}},
			code:      1, stderr: "remaining firewall-rules: " + testPrefix + "-ssh",
		},
		{
			name:      "bucket remains",
			resources: []mockResource{{Kind: "buckets", Name: testPrefix + "-x", Labels: runLabels()}},
			code:      1, stderr: "remaining buckets",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, mockState{Resources: tt.resources, Fail: tt.fail})
			r := l.run("verify-empty.sh")
			if r.code != tt.code || !strings.Contains(r.stderr, tt.stderr) {
				t.Fatalf("exit=%d stderr=%q; want exit=%d containing %q", r.code, r.stderr, tt.code, tt.stderr)
			}
		})
	}
}

func TestActionLogIsAppendOnly(t *testing.T) {
	l := newLab(t, mockState{})
	logPath := filepath.Join(l.env["HW_LOG_DIR"], "actions.log")
	if r := l.run("verify-empty.sh"); r.code != 0 {
		t.Fatalf("first run: %q", r.stderr)
	}
	first, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if r := l.run("verify-empty.sh"); r.code != 0 {
		t.Fatalf("second run: %q", r.stderr)
	}
	second, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(second, first) || len(second) <= len(first) {
		t.Fatalf("log was rewritten:\nfirst=%q\nsecond=%q", first, second)
	}
	if n := strings.Count(string(second), "\tverify-empty\tstart\t"); n != 2 {
		t.Fatalf("got %d start records, want 2", n)
	}

	if err := os.Remove(logPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(l.dir, "elsewhere"), logPath); err != nil {
		t.Fatal(err)
	}
	if r := l.run("verify-empty.sh"); r.code == 0 || !strings.Contains(r.stderr, "must not be a symlink") {
		t.Fatalf("symlinked log accepted: exit=%d stderr=%q", r.code, r.stderr)
	}
}

func TestShellcheck(t *testing.T) {
	shellcheck, err := exec.LookPath("shellcheck")
	if err != nil {
		t.Skip("shellcheck is not installed")
	}
	files, err := filepath.Glob("*.sh")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, shellcheck, append([]string{"--external-sources", "--source-path=SCRIPTDIR"}, files...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("shellcheck: %v\n%s", err, out)
	}
}

const testPrices = `# key usd-per-unit-hour (test values, not real prices)
c4a-highmem-96-metal 10
c4a-standard-8 0.5
hyperdisk-balanced-gib 0.0001
cloud-nat-gateway 0.05
fixed-overhead 0.2
`

// provisionLab returns a lab configured for provision.sh with test prices.
func provisionLab(t *testing.T, state mockState) *lab {
	t.Helper()
	l := newLab(t, state)
	prices := filepath.Join(l.dir, "prices.txt")
	if err := os.WriteFile(prices, []byte(testPrices), 0o600); err != nil {
		t.Fatal(err)
	}
	l.env["PRICE_TABLE"] = prices
	l.env["PLANNED_HOURS"] = "4"
	l.env["METAL_COUNT"] = "2"
	l.env["STANDARD_COUNT"] = "1"
	return l
}

func (s mockState) get(kind, name string) (mockResource, bool) {
	for _, r := range s.Resources {
		if r.Kind == kind && r.Name == name {
			return r, true
		}
	}
	return mockResource{}, false
}

func TestProvisionInputs(t *testing.T) {
	for _, tt := range []struct {
		name   string
		env    map[string]string
		prices string
		stderr string
	}{
		{name: "missing price table", env: map[string]string{"PRICE_TABLE": ""}, stderr: "required input PRICE_TABLE"},
		{name: "absent price table", env: map[string]string{"PRICE_TABLE": "/nonexistent/prices"}, stderr: "missing or unreadable"},
		{name: "placeholder price", prices: strings.Replace(testPrices, "c4a-standard-8 0.5", "c4a-standard-8 <USD>", 1), stderr: "not a USD amount"},
		{name: "missing price key", prices: strings.Replace(testPrices, "cloud-nat-gateway 0.05\n", "", 1), stderr: "define cloud-nat-gateway exactly once"},
		{name: "duplicate price key", prices: testPrices + "fixed-overhead 0\n", stderr: "define fixed-overhead exactly once"},
		{name: "missing hours", env: map[string]string{"PLANNED_HOURS": ""}, stderr: "required input PLANNED_HOURS"},
		{name: "zero hours", env: map[string]string{"PLANNED_HOURS": "0"}, stderr: "PLANNED_HOURS must be a positive number"},
		{name: "missing metal count", env: map[string]string{"METAL_COUNT": ""}, stderr: "required input METAL_COUNT"},
		{name: "five metal hosts", env: map[string]string{"METAL_COUNT": "5"}, stderr: "between 1 and 4"},
		{name: "no metal hosts", env: map[string]string{"METAL_COUNT": "0"}, stderr: "between 1 and 4"},
		{name: "three standard hosts", env: map[string]string{"STANDARD_COUNT": "3"}, stderr: "at most 2"},
		{name: "over ceiling", env: map[string]string{"SPEND_CEILING_USD": "50"}, stderr: "exceeds ceiling 50 USD"},
		{name: "over ceiling by hours", env: map[string]string{"PLANNED_HOURS": "48"}, stderr: "exceeds ceiling 500 USD"},
		{name: "over ceiling by disks", env: map[string]string{"DATA_DISK_GB": "99999", "SPEND_CEILING_USD": "90"}, stderr: "exceeds ceiling"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := provisionLab(t, mockState{})
			if tt.prices != "" {
				if err := os.WriteFile(l.env["PRICE_TABLE"], []byte(tt.prices), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			for k, v := range tt.env {
				l.env[k] = v
			}
			r := l.run("provision.sh")
			if r.code == 0 || !strings.Contains(r.stderr, tt.stderr) {
				t.Fatalf("exit=%d stderr=%q; want failure containing %q", r.code, r.stderr, tt.stderr)
			}
			if c := l.commands(); len(c) != 0 {
				t.Fatalf("refused provision made gcloud calls: %q", c)
			}
		})
	}
}

func TestProvision(t *testing.T) {
	for _, tt := range []struct {
		name     string
		stockout []string
		zone     string
	}{
		{name: "primary zone", zone: testZone},
		{name: "stockout falls back to second zone", stockout: []string{testZone}, zone: testZone2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := provisionLab(t, mockState{StockoutZones: tt.stockout})
			r := l.run("provision.sh")
			if r.code != 0 {
				t.Fatalf("exit=%d stdout=%q stderr=%q", r.code, r.stdout, r.stderr)
			}
			if !strings.Contains(r.stdout, "plan projects 83.28 USD over 4 h") {
				t.Errorf("stdout=%q; want the projected spend", r.stdout)
			}
			state := l.state()
			for _, name := range []string{"std-1", "metal-1", "metal-2"} {
				inst, ok := state.get("instances", testPrefix+"-"+name)
				if !ok {
					t.Fatalf("instance %s missing", name)
				}
				if inst.Zone != tt.zone || inst.Labels["brie-hw-run"] != testRunID {
					t.Errorf("instance %s: zone=%s labels=%v", name, inst.Zone, inst.Labels)
				}
				if !strings.Contains(inst.Flags["network-interface"], "no-address") ||
					!hasFlag(inst.Flags, "no-service-account") {
					t.Errorf("instance %s has an external address or credentials: %v", name, inst.Flags)
				}
				boot, _ := state.get("disks", inst.Name)
				if boot.Labels["brie-hw-run"] != testRunID {
					t.Errorf("boot disk %s is not labelled: %v", inst.Name, boot.Labels)
				}
			}
			metal, _ := state.get("instances", testPrefix+"-metal-1")
			if metal.Flags["machine-type"] != "c4a-highmem-96-metal" || !strings.Contains(metal.Flags["network-interface"], "nic-type=IDPF") {
				t.Errorf("metal flags: %v", metal.Flags)
			}
			for _, name := range []string{"metal-1", "metal-2"} {
				disk, ok := state.get("disks", testPrefix+"-"+name+"-data")
				if !ok || disk.Zone != tt.zone || disk.Labels["brie-hw-run"] != testRunID || len(disk.Users) != 1 {
					t.Errorf("data disk for %s: %+v", name, disk)
				}
			}
			if _, ok := state.get("disks", testPrefix+"-std-1-data"); ok {
				t.Error("standard host received a data disk")
			}
			bucket, ok := state.get("buckets", testPrefix+"-"+testProject)
			if !ok || bucket.Labels["brie-hw-run"] != testRunID || !hasFlag(bucket.Flags, "uniform-bucket-level-access") ||
				bucket.Flags["lifecycle-file"] == "" {
				t.Fatalf("bucket: %+v", bucket)
			}
			lifecycle, err := os.ReadFile(bucket.Flags["lifecycle-file"])
			if err != nil || !strings.Contains(string(lifecycle), `"action":{"type":"Delete"},"condition":{"age":7}`) {
				t.Errorf("lifecycle=%q err=%v", lifecycle, err)
			}
			for _, kind := range []string{"networks", "subnets", "routers", "firewall-rules"} {
				for _, res := range state.Resources {
					if res.Kind == kind && res.Description != testMarker {
						t.Errorf("%s %s lacks the run marker", kind, res.Name)
					}
				}
			}
			if _, ok := state.get("nats", testPrefix+"-nat"); !ok {
				t.Error("NAT missing")
			}
			for _, res := range state.Resources {
				if !strings.HasPrefix(res.Name, testPrefix+"-") {
					t.Errorf("resource %s/%s lacks the run prefix", res.Kind, res.Name)
				}
			}
			inventory, err := os.ReadFile(filepath.Join(l.env["HW_LOG_DIR"], "inventory.tsv"))
			if err != nil || strings.Count(string(inventory), "\t"+tt.zone+"\n") != 3 {
				t.Errorf("inventory=%q err=%v", inventory, err)
			}

			// A second run with the same RUN_ID refuses before creating anything.
			before := len(l.commands())
			if r := l.run("provision.sh"); r.code == 0 || !strings.Contains(r.stderr, "already exists") {
				t.Errorf("rerun: exit=%d stderr=%q", r.code, r.stderr)
			}
			if after := len(l.commands()); after != before {
				t.Errorf("rerun made %d gcloud calls", after-before)
			}

			// The provisioned lab tears down completely.
			if r := l.run("teardown.sh"); r.code != 0 {
				t.Fatalf("teardown: exit=%d stderr=%q", r.code, r.stderr)
			}
			if left := l.state().Resources; len(left) != 0 {
				t.Fatalf("resources left after teardown: %+v", left)
			}
		})
	}
}

func hasFlag(flags map[string]string, name string) bool {
	_, ok := flags[name]
	return ok
}

func TestProvisionStockoutAbort(t *testing.T) {
	l := provisionLab(t, mockState{StockoutZones: []string{testZone, testZone2}})
	r := l.run("provision.sh")
	if r.code == 0 || !strings.Contains(r.stderr, "stockout cap reached") {
		t.Fatalf("exit=%d stderr=%q; want stockout abort", r.code, r.stderr)
	}
	if !strings.Contains(r.stdout+r.stderr, "teardown: all resources for run "+testRunID+" deleted and verified empty") {
		t.Errorf("abort did not complete teardown: stdout=%q stderr=%q", r.stdout, r.stderr)
	}
	if left := l.state().Resources; len(left) != 0 {
		t.Fatalf("resources left after abort: %+v", left)
	}
	if elapsed := l.clock() - testClockStart; elapsed < 2700 || elapsed > 2700+60 {
		t.Errorf("aborted after %d simulated seconds, want the 45-minute cap", elapsed)
	}
	commands := l.commands()
	primary := indexOf(commands, `^compute instances create \S+ --zone=`+testZone)
	fallback := indexOf(commands, `^compute instances create \S+ --zone=`+testZone2)
	if primary < 0 || fallback < primary {
		t.Errorf("zone order: primary=%d fallback=%d", primary, fallback)
	}
	if indexOf(commands, `^compute (disks|instances) create .*metal`) >= 0 {
		t.Error("created a metal host or disk despite the stockout")
	}
}

func TestFence(t *testing.T) {
	source := testPrefix + "-metal-1"
	url := instanceURL(testProject, testZone, source)
	instance := func(labels map[string]string) mockResource {
		return mockResource{Kind: "instances", Name: source, Zone: testZone, Labels: labels,
			Status: "RUNNING", ID: "9001", Created: "2026-11-13T11:00:00.000-08:00"}
	}
	dataDisk := func(labels map[string]string, users ...string) mockResource {
		return mockResource{Kind: "disks", Name: source + "-data", Zone: testZone, Labels: labels, Users: users}
	}
	for _, tt := range []struct {
		name       string
		state      mockState
		args       []string
		verified   bool
		reason     string
		stopped    bool
		detached   bool
		minElapsed int64
	}{
		{
			name:     "verified",
			state:    mockState{Resources: []mockResource{instance(runLabels()), dataDisk(runLabels(), url)}},
			verified: true, reason: "TERMINATED observed", stopped: true, detached: true,
		},
		{
			name:     "disk already detached",
			state:    mockState{Resources: []mockResource{instance(runLabels()), dataDisk(runLabels())}},
			verified: true, reason: "TERMINATED observed", stopped: true,
		},
		{
			name:   "unlabelled instance",
			state:  mockState{Resources: []mockResource{instance(nil), dataDisk(runLabels(), url)}},
			reason: "does not carry label " + testMarker,
		},
		{
			name:   "instance of another run",
			state:  mockState{Resources: []mockResource{instance(map[string]string{"brie-hw-run": "r20261112"}), dataDisk(runLabels(), url)}},
			reason: "does not carry label " + testMarker,
		},
		{
			name:   "missing instance",
			state:  mockState{},
			reason: "cannot describe instance",
		},
		{
			name: "never terminated",
			state: mockState{StopStatus: "STOPPING",
				Resources: []mockResource{instance(runLabels()), dataDisk(runLabels(), url)}},
			reason: "status STOPPING did not reach TERMINATED within 600 s", stopped: true, minElapsed: 600,
		},
		{
			name:   "unlabelled data disk",
			state:  mockState{Resources: []mockResource{instance(runLabels()), dataDisk(nil, url)}},
			reason: "data disk " + source + "-data does not carry label", stopped: true,
		},
		{
			name:   "missing data disk",
			state:  mockState{Resources: []mockResource{instance(runLabels())}},
			reason: "cannot describe data disk", stopped: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newLab(t, tt.state)
			r := l.run("fence.sh", source, testZone)
			if (r.code == 0) != tt.verified {
				t.Fatalf("exit=%d stdout=%q stderr=%q; want verified=%t", r.code, r.stdout, r.stderr, tt.verified)
			}
			var evidence map[string]any
			if err := json.Unmarshal([]byte(r.stdout), &evidence); err != nil {
				t.Fatalf("evidence is not JSON: %v: %q", err, r.stdout)
			}
			if evidence["verified"] != tt.verified || !strings.Contains(evidence["reason"].(string), tt.reason) {
				t.Errorf("evidence=%v; want verified=%t reason containing %q", evidence, tt.verified, tt.reason)
			}
			if evidence["schema"] != "brie.hw.fence/v1" || evidence["instance"] != source || evidence["run_id"] != testRunID {
				t.Errorf("evidence identity: %v", evidence)
			}
			if tt.verified {
				for key, want := range map[string]string{
					"instance_id": "9001", "creation_timestamp": "2026-11-13T11:00:00.000-08:00",
					"final_status": "TERMINATED", "disk_users_after": "",
				} {
					if evidence[key] != want {
						t.Errorf("%s=%v, want %q", key, evidence[key], want)
					}
				}
				for _, key := range []string{"requested_at", "stop_returned_at", "terminated_observed_at", "disk_detached_at"} {
					if _, err := time.Parse(time.RFC3339, evidence[key].(string)); err != nil {
						t.Errorf("%s=%v is not a timestamp", key, evidence[key])
					}
				}
			}
			commands := l.commands()
			if stopped := indexOf(commands, `^compute instances stop `+source) >= 0; stopped != tt.stopped {
				t.Errorf("stop issued=%t, want %t: %q", stopped, tt.stopped, commands)
			}
			if detached := indexOf(commands, `^compute instances detach-disk `+source) >= 0; detached != tt.detached {
				t.Errorf("detach issued=%t, want %t: %q", detached, tt.detached, commands)
			}
			if elapsed := l.clock() - testClockStart; elapsed < tt.minElapsed {
				t.Errorf("gave up after %d simulated seconds, want at least %d", elapsed, tt.minElapsed)
			}
			saved, err := os.ReadFile(filepath.Join(l.env["HW_LOG_DIR"], "fence-evidence.jsonl"))
			if err != nil || strings.TrimSpace(string(saved)) != strings.TrimSpace(r.stdout) {
				t.Errorf("saved evidence=%q err=%v", saved, err)
			}
		})
	}

	t.Run("usage", func(t *testing.T) {
		l := newLab(t, mockState{})
		if r := l.run("fence.sh", source); r.code == 0 || !strings.Contains(r.stderr, "usage: fence.sh INSTANCE ZONE") {
			t.Fatalf("exit=%d stderr=%q", r.code, r.stderr)
		}
	})
}

func (l *lab) clock() int64 {
	l.t.Helper()
	data, err := os.ReadFile(l.env[mockClockEnv])
	if err != nil {
		l.t.Fatal(err)
	}
	var now int64
	if _, err := fmt.Sscan(string(data), &now); err != nil {
		l.t.Fatal(err)
	}
	return now
}

func TestProvisionFailureTearsDown(t *testing.T) {
	bucket := testPrefix + "-" + testProject
	for _, tt := range []struct {
		fail     string
		retained []string
		stderr   string
	}{
		{fail: "compute routers nats create"},
		{fail: "compute instances attach-disk"},
		// The bucket is labelled only after creation, so a failed label
		// update leaves an unmarked bucket that teardown must refuse.
		{fail: "storage buckets update", retained: []string{"buckets/" + bucket}, stderr: "REFUSED bucket gs://" + bucket},
	} {
		t.Run(tt.fail, func(t *testing.T) {
			l := provisionLab(t, mockState{Fail: []string{tt.fail}})
			r := l.run("provision.sh")
			if r.code == 0 || !strings.Contains(r.stderr, "provision: failed; running teardown") {
				t.Fatalf("exit=%d stderr=%q; want a failed provision that tears down", r.code, r.stderr)
			}
			if tt.retained != nil && (!strings.Contains(r.stderr, tt.stderr) || !strings.Contains(r.stderr, "TEARDOWN INCOMPLETE")) {
				t.Errorf("stderr=%q; want %q and an incomplete teardown", r.stderr, tt.stderr)
			}
			var left []string
			for _, res := range l.state().Resources {
				left = append(left, res.Kind+"/"+res.Name)
			}
			if strings.Join(left, ",") != strings.Join(tt.retained, ",") {
				t.Fatalf("resources left after failed provision: %v, want %v", left, tt.retained)
			}
		})
	}
}
