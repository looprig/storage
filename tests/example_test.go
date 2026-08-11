package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const offlineCommand = "GOWORK=off go test -race ./..."

type manifest struct {
	SchemaVersion int           `json:"schemaVersion"`
	Repository    string        `json:"repository"`
	ProofSources  []proofSource `json:"proofSources"`
	Examples      []example     `json:"examples"`
}

type proofSource struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Path   string `json:"path"`
	Symbol string `json:"symbol,omitempty"`
}

type example struct {
	ID             string            `json:"id"`
	Ecosystem      string            `json:"ecosystem"`
	Owner          string            `json:"owner"`
	SourcePath     string            `json:"sourcePath"`
	Availability   string            `json:"availability"`
	Versions       map[string]string `json:"versions"`
	OfflineCommand string            `json:"offlineCommand"`
	Assertion      string            `json:"assertion"`
	WorkflowPath   string            `json:"workflowPath"`
	JobID          string            `json:"jobId"`
	Cleanup        string            `json:"cleanup"`
	LiveGate       any               `json:"liveGate"`
	ProofIDs       []string          `json:"proofIds"`
}

func TestRunnableExamplesExist(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"examples/ledger/example_test.go",
		"examples/leases/example_test.go",
		"examples/kv/example_test.go",
		"examples/blobs/example_test.go",
		"examples/composite/example_test.go",
		"examples/storetest/example_test.go",
	} {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			if _, err := os.Stat(filepath.Join("..", path)); err != nil {
				t.Fatalf("runnable example %q: %v", path, err)
			}
		})
	}
}

func TestDocsExamplesArtifacts(t *testing.T) {
	t.Parallel()
	root := ".."
	data, err := os.ReadFile(filepath.Join(root, "testdata/docs/examples.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got manifest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Repository != "storage" {
		t.Fatalf("manifest identity = (%d, %q)", got.SchemaVersion, got.Repository)
	}
	if len(got.Examples) != 6 {
		t.Fatalf("manifest has %d examples, want 6", len(got.Examples))
	}

	proofs := make(map[string]struct{}, len(got.ProofSources))
	for _, proof := range got.ProofSources {
		if !strings.HasPrefix(proof.ID, "example-storage-") || proof.Path == "" || proof.Symbol == "" {
			t.Errorf("invalid proof source: %#v", proof)
		}
		if proof.Type != "executable-fixture" && proof.Type != "test" {
			t.Errorf("proof source %q type = %q", proof.ID, proof.Type)
		}
		if strings.Contains(proof.Path, "#") {
			t.Errorf("proof source %q path contains a symbol fragment: %q", proof.ID, proof.Path)
		}
		if _, duplicate := proofs[proof.ID]; duplicate {
			t.Errorf("duplicate proof source %q", proof.ID)
		}
		proofs[proof.ID] = struct{}{}
		if _, err := os.Stat(filepath.Join(root, proof.Path)); err != nil {
			t.Errorf("proof source %q: %v", proof.ID, err)
		}
	}

	workflowData, err := os.ReadFile(filepath.Join(root, ".github/workflows/docs-examples.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(workflowData)
	for _, required := range []string{"docs-examples:", "run: " + offlineCommand, "run: GOWORK=off make check", "GOCACHE:"} {
		if !strings.Contains(workflow, required) {
			t.Errorf("workflow does not contain %q", required)
		}
	}

	seen := make(map[string]struct{}, len(got.Examples))
	for _, item := range got.Examples {
		if !strings.HasPrefix(item.ID, "example-storage-") {
			t.Errorf("example ID %q is not globally namespaced", item.ID)
		}
		if _, duplicate := seen[item.ID]; duplicate {
			t.Errorf("duplicate example ID %q", item.ID)
		}
		seen[item.ID] = struct{}{}
		if item.Ecosystem != "go" || item.Owner != "storage" || item.Availability != "source-workspace" {
			t.Errorf("example %q identity fields are invalid", item.ID)
		}
		if len(item.Versions) != 1 || item.Versions["github.com/looprig/storage"] != "source-workspace" {
			t.Errorf("example %q versions = %#v", item.ID, item.Versions)
		}
		if item.OfflineCommand != offlineCommand || item.Assertion == "" || item.Cleanup == "" {
			t.Errorf("example %q execution metadata is incomplete", item.ID)
		}
		if item.SourcePath == "" || item.WorkflowPath != ".github/workflows/docs-examples.yml" || item.JobID != "docs-examples" || item.LiveGate != nil {
			t.Errorf("example %q source/automation metadata is invalid", item.ID)
		}
		if _, err := os.Stat(filepath.Join(root, item.SourcePath)); err != nil {
			t.Errorf("example %q source: %v", item.ID, err)
		}
		if len(item.ProofIDs) == 0 {
			t.Errorf("example %q has no proof IDs", item.ID)
		}
		for _, id := range item.ProofIDs {
			if _, ok := proofs[id]; !ok {
				t.Errorf("example %q references unknown proof %q", item.ID, id)
			}
		}
	}
}
