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
		"date": "#!/bin/sh\nnow=$(cat \"$BRIE_HW_MOCK_CLOCK\")\ncase \"$*\" in\n" +
			"*+%s*) echo \"$now\" ;;\n" +
			"*'+%Y-%m-%dT%H:%M:%SZ'*) exec '" + realDate + "' -u -d \"@$now\" +%Y-%m-%dT%H:%M:%SZ ;;\n" +
			"*) echo \"mock date: unsupported $*\" >&2; exit 2 ;;\nesac\n",
		"sleep": "#!/bin/sh\nnow=$(cat \"$BRIE_HW_MOCK_CLOCK\")\necho $((now + $1)) >\"$BRIE_HW_MOCK_CLOCK\"\n",
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
	if r := l.runCommand("sh", "-c", "command -v gcloud"); strings.TrimSpace(r.stdout) != filepath.Join(bin, "gcloud") {
		t.Fatalf("gcloud resolves to %q, not the mock; refusing to run scripts", r.stdout)
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

var scripts = []string{"verify-empty.sh", "teardown.sh"}

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
			l := newLab(t, mockState{Resources: append(resources, tt.extra...)})
			r := l.run("teardown.sh")
			if r.code != tt.code {
				t.Fatalf("exit=%d, want %d; stdout=%q stderr=%q", r.code, tt.code, r.stdout, r.stderr)
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
		code      int
		stderr    string
	}{
		{name: "empty", resources: []mockResource{{Kind: "networks", Name: "default"}}},
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
			l := newLab(t, mockState{Resources: tt.resources})
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
