// Copyright 2026 The Brie Authors.
// SPDX-License-Identifier: Apache-2.0

package hw_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The hardware lab scripts are tested only against this mock. It is the test
// binary re-executed through a wrapper named gcloud placed first on PATH. It
// records every argv, enforces the explicit-project guard, and simulates the
// resources a lab run would create.

const (
	mockToolEnv  = "BRIE_HW_MOCK_TOOL"
	mockStateEnv = "BRIE_HW_MOCK_STATE"
	mockLogEnv   = "BRIE_HW_MOCK_LOG"
	mockClockEnv = "BRIE_HW_MOCK_CLOCK"

	exitMissingProject = 97
	exitConfigCommand  = 96
)

type mockResource struct {
	Kind        string            `json:"kind"`
	Name        string            `json:"name"`
	Zone        string            `json:"zone,omitempty"`
	Region      string            `json:"region,omitempty"`
	Router      string            `json:"router,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Description string            `json:"description,omitempty"`
	Status      string            `json:"status,omitempty"`
	ID          string            `json:"id,omitempty"`
	Created     string            `json:"created,omitempty"`
	Users       []string          `json:"users,omitempty"`
	Flags       map[string]string `json:"flags,omitempty"`
}

type mockState struct {
	Project   string         `json:"project"`
	Resources []mockResource `json:"resources"`
	// StockoutZones lists zones whose instance creates fail with a stockout.
	StockoutZones []string `json:"stockoutZones,omitempty"`
	// StopStatus is the status an instance reports after stop; empty means
	// TERMINATED.
	StopStatus string `json:"stopStatus,omitempty"`
	// SSH answers gcloud compute ssh by its --command; Kubectl answers
	// kubectl by its joined arguments. Unmatched calls succeed silently.
	SSH     []mockResponse `json:"ssh,omitempty"`
	Kubectl []mockResponse `json:"kubectl,omitempty"`
	NextID  int            `json:"nextID"`
	// Fail makes any gcloud call whose joined arguments contain one of these
	// substrings exit 1 without side effects.
	Fail []string `json:"fail,omitempty"`
}

// mockResponse answers the first call containing Match. A positive Times
// limits how many calls it answers.
type mockResponse struct {
	Match  string `json:"match"`
	Output string `json:"output,omitempty"`
	Exit   int    `json:"exit,omitempty"`
	Times  int    `json:"times,omitempty"`
}

// respond writes the first matching response and returns its exit status.
func respond(responses []mockResponse, call string) ([]mockResponse, int) {
	for i, r := range responses {
		if !strings.Contains(call, r.Match) {
			continue
		}
		fmt.Print(r.Output)
		if r.Times > 0 {
			responses[i].Times--
			if responses[i].Times == 0 {
				responses = append(responses[:i], responses[i+1:]...)
			}
		}
		return responses, r.Exit
	}
	return responses, 0
}

type mockCall struct {
	Tool string   `json:"tool"`
	Args []string `json:"args"`
}

func runMock(tool string, args []string) int {
	if err := appendMockCall(tool, args); err != nil {
		fmt.Fprintf(os.Stderr, "mock %s: %v\n", tool, err)
		return 90
	}
	switch tool {
	case "gcloud":
		return runMockGcloud(args)
	case "kubectl":
		return runMockKubectl(args)
	default:
		fmt.Fprintf(os.Stderr, "mock: unknown tool %q\n", tool)
		return 90
	}
}

func appendMockCall(tool string, args []string) error {
	line, err := json.Marshal(mockCall{Tool: tool, Args: args})
	if err != nil {
		return err
	}
	f, err := os.OpenFile(os.Getenv(mockLogEnv), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func loadMockState(path string) (*mockState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state mockState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func saveMockState(path string, state *mockState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

type gcloudCommand struct {
	words []string
	flags map[string]string
}

func parseGcloud(args []string) (gcloudCommand, string, error) {
	cmd := gcloudCommand{flags: map[string]string{}}
	project := ""
	for i, arg := range args {
		if arg == "--" {
			cmd.flags["--"] = strings.Join(args[i+1:], " ")
			break
		}
		if !strings.HasPrefix(arg, "--") {
			cmd.words = append(cmd.words, arg)
			continue
		}
		key, value, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		if key == "project" {
			project = value
			continue
		}
		cmd.flags[key] = value
	}
	if project == "" {
		return cmd, "", errors.New("missing explicit --project")
	}
	return cmd, project, nil
}

func runMockGcloud(args []string) int {
	cmd, project, err := parseGcloud(args)
	if len(cmd.words) > 0 && cmd.words[0] == "config" {
		fmt.Fprintln(os.Stderr, "mock gcloud: gcloud config must never be used")
		return exitConfigCommand
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "mock gcloud: %v\n", err)
		return exitMissingProject
	}
	statePath := os.Getenv(mockStateEnv)
	state, err := loadMockState(statePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mock gcloud: %v\n", err)
		return 90
	}
	if project != state.Project {
		fmt.Fprintf(os.Stderr, "mock gcloud: unexpected project %q\n", project)
		return exitMissingProject
	}
	joined := strings.Join(args, " ")
	for _, f := range state.Fail {
		if strings.Contains(joined, f) {
			fmt.Fprintf(os.Stderr, "ERROR: (gcloud) simulated failure for %q\n", f)
			return 1
		}
	}
	code := state.handle(cmd)
	if err := saveMockState(statePath, state); err != nil {
		fmt.Fprintf(os.Stderr, "mock gcloud: %v\n", err)
		return 90
	}
	return code
}

func runMockKubectl(args []string) int {
	if len(args) == 0 || !strings.HasPrefix(args[0], "--kubeconfig=/") {
		fmt.Fprintln(os.Stderr, "mock kubectl: missing explicit --kubeconfig")
		return exitMissingProject
	}
	statePath := os.Getenv(mockStateEnv)
	state, err := loadMockState(statePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mock kubectl: %v\n", err)
		return 90
	}
	var code int
	state.Kubectl, code = respond(state.Kubectl, strings.Join(args, " "))
	if err := saveMockState(statePath, state); err != nil {
		fmt.Fprintf(os.Stderr, "mock kubectl: %v\n", err)
		return 90
	}
	return code
}

// resourceRef splits the command words into kind, verb, and name.
func resourceRef(words []string) (kind, verb, name string) {
	at := func(i int) string {
		if i < len(words) {
			return words[i]
		}
		return ""
	}
	switch {
	case at(0) == "storage" && at(1) == "rm":
		return "buckets", "rm", strings.TrimPrefix(at(2), "gs://")
	case at(0) == "storage":
		return at(1), at(2), strings.TrimPrefix(at(3), "gs://")
	case at(1) == "networks" && at(2) == "subnets":
		return "subnets", at(3), at(4)
	case at(1) == "routers" && at(2) == "nats":
		return "nats", at(3), at(4)
	case at(1) == "ssh" || at(1) == "scp":
		return at(1), at(1), at(2)
	default:
		return at(1), at(2), at(3)
	}
}

func (s *mockState) find(kind, name string, flags map[string]string) int {
	for i, r := range s.Resources {
		if r.Kind != kind || r.Name != name {
			continue
		}
		if zone, ok := flags["zone"]; ok && r.Zone != zone {
			continue
		}
		if region, ok := flags["region"]; ok && r.Region != region {
			continue
		}
		if router, ok := flags["router"]; ok && r.Router != router {
			continue
		}
		return i
	}
	return -1
}

func (s *mockState) now() string {
	return time.Unix(mockClock(), 0).UTC().Format(time.RFC3339)
}

func (s *mockState) handle(cmd gcloudCommand) int {
	kind, verb, name := resourceRef(cmd.words)
	switch verb {
	case "ssh":
		var code int
		s.SSH, code = respond(s.SSH, cmd.flags["command"])
		return code
	case "scp":
		return 0
	case "list":
		return s.list(kind, cmd.flags)
	case "describe":
		i := s.find(kind, name, cmd.flags)
		if i < 0 {
			fmt.Fprintf(os.Stderr, "ERROR: (gcloud) %s %s was not found\n", kind, name)
			return 1
		}
		fmt.Println(render(s.Resources[i], cmd.flags["format"]))
		return 0
	case "create":
		return s.create(kind, name, cmd.flags)
	case "delete", "rm":
		i := s.find(kind, name, cmd.flags)
		if i < 0 {
			fmt.Fprintf(os.Stderr, "ERROR: (gcloud) %s %s was not found\n", kind, name)
			return 1
		}
		s.Resources = append(s.Resources[:i], s.Resources[i+1:]...)
		return 0
	case "stop":
		i := s.find(kind, name, cmd.flags)
		if i < 0 {
			return 1
		}
		s.Resources[i].Status = "TERMINATED"
		if s.StopStatus != "" {
			s.Resources[i].Status = s.StopStatus
		}
		return 0
	case "add-labels", "update":
		i := s.find(kind, name, cmd.flags)
		if i < 0 {
			return 1
		}
		labels := cmd.flags["labels"] + cmd.flags["update-labels"]
		for k, v := range parseLabels(labels) {
			if s.Resources[i].Labels == nil {
				s.Resources[i].Labels = map[string]string{}
			}
			s.Resources[i].Labels[k] = v
		}
		for k, v := range cmd.flags {
			if s.Resources[i].Flags == nil {
				s.Resources[i].Flags = map[string]string{}
			}
			s.Resources[i].Flags[k] = v
		}
		return 0
	case "attach-disk", "detach-disk":
		disk := s.find("disks", cmd.flags["disk"], map[string]string{"zone": cmd.flags["zone"]})
		if disk < 0 || s.find("instances", name, cmd.flags) < 0 {
			return 1
		}
		user := instanceURL(s.Project, cmd.flags["zone"], name)
		users := s.Resources[disk].Users[:0:0]
		for _, u := range s.Resources[disk].Users {
			if u != user {
				users = append(users, u)
			}
		}
		if verb == "attach-disk" {
			users = append(users, user)
		}
		s.Resources[disk].Users = users
		return 0
	default:
		fmt.Fprintf(os.Stderr, "mock gcloud: unsupported command %q\n", cmd.words)
		return 91
	}
}

func instanceURL(project, zone, name string) string {
	return "https://www.googleapis.com/compute/v1/projects/" + project + "/zones/" + zone + "/instances/" + name
}

func parseLabels(value string) map[string]string {
	labels := map[string]string{}
	for _, pair := range strings.Split(value, ",") {
		if k, v, ok := strings.Cut(pair, "="); ok {
			labels[k] = v
		}
	}
	return labels
}

func (s *mockState) create(kind, name string, flags map[string]string) int {
	if s.find(kind, name, flags) >= 0 {
		fmt.Fprintf(os.Stderr, "ERROR: (gcloud) %s %s already exists\n", kind, name)
		return 1
	}
	zone := flags["zone"]
	if kind == "instances" {
		for _, z := range s.StockoutZones {
			if z == zone {
				fmt.Fprintf(os.Stderr, "ERROR: (gcloud.compute.instances.create) Could not fetch resource:\n"+
					" - The zone 'projects/%s/zones/%s' does not have enough resources available to fulfill the request.  "+
					"Try a different zone, or try again later. (ZONE_RESOURCE_POOL_EXHAUSTED)\n", s.Project, zone)
				return 1
			}
		}
	}
	s.NextID++
	r := mockResource{
		Kind: kind, Name: name, Zone: zone, Region: flags["region"], Router: flags["router"],
		Description: flags["description"], ID: fmt.Sprintf("%d", 7000+s.NextID), Created: s.now(),
		Flags: flags,
	}
	if labels := flags["labels"]; labels != "" {
		r.Labels = parseLabels(labels)
	}
	if kind == "instances" {
		r.Status = "RUNNING"
		s.Resources = append(s.Resources, mockResource{
			Kind: "disks", Name: name, Zone: zone, Users: []string{instanceURL(s.Project, zone, name)},
		})
	}
	s.Resources = append(s.Resources, r)
	return 0
}

var (
	labelFilter  = regexp.MustCompile(`labels\.brie-hw-run=([a-z0-9]+)`)
	prefixFilter = regexp.MustCompile(`name ~ "\^([^"]+)"`)
)

func (s *mockState) list(kind string, flags map[string]string) int {
	filter := flags["filter"]
	label := labelFilter.FindStringSubmatch(filter)
	prefix := prefixFilter.FindStringSubmatch(filter)
	if filter != "" && label == nil && prefix == nil {
		fmt.Fprintf(os.Stderr, "mock gcloud: unsupported filter %q\n", filter)
		return 91
	}
	for _, r := range s.Resources {
		if r.Kind != kind {
			continue
		}
		if router := flags["router"]; kind == "nats" && r.Router != router {
			continue
		}
		matched := filter == ""
		if label != nil && r.Labels["brie-hw-run"] == label[1] {
			matched = true
		}
		if prefix != nil && strings.HasPrefix(r.Name, prefix[1]) {
			matched = true
		}
		if matched {
			fmt.Println(render(r, flags["format"]))
		}
	}
	return 0
}

var valueFormat = regexp.MustCompile(`^value\((.*)\)$`)

func render(r mockResource, format string) string {
	m := valueFormat.FindStringSubmatch(format)
	if m == nil {
		return r.Name
	}
	var fields []string
	for _, field := range strings.Split(m[1], ",") {
		switch field {
		case "name":
			fields = append(fields, r.Name)
		case "zone.basename()":
			fields = append(fields, r.Zone)
		case "region.basename()":
			fields = append(fields, r.Region)
		case "labels":
			var pairs []string
			for k, v := range r.Labels {
				pairs = append(pairs, k+"="+v)
			}
			sort.Strings(pairs)
			fields = append(fields, strings.Join(pairs, ";"))
		case "description":
			fields = append(fields, r.Description)
		case "status":
			fields = append(fields, r.Status)
		case "id":
			fields = append(fields, r.ID)
		case "creationTimestamp":
			fields = append(fields, r.Created)
		case "users":
			fields = append(fields, strings.Join(r.Users, ";"))
		default:
			fields = append(fields, "UNSUPPORTED-FIELD-"+field)
		}
	}
	return strings.Join(fields, "\t")
}

func mockClock() int64 {
	data, err := os.ReadFile(os.Getenv(mockClockEnv))
	if err != nil {
		return 0
	}
	var now int64
	_, _ = fmt.Sscan(string(data), &now)
	return now
}
