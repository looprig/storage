package storage

import (
	"reflect"
	"testing"
)

type fakePathReporter struct {
	paths []string
}

func (r fakePathReporter) StoragePaths() []string { return r.paths }

var _ PathReporter = fakePathReporter{}

func TestPathReporter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		paths []string
	}{
		{name: "no local paths", paths: nil},
		{name: "one local path", paths: []string{"/var/lib/looprig"}},
		{name: "multiple local paths", paths: []string{"/var/lib/looprig", "/mnt/looprig"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var reporter PathReporter = fakePathReporter{paths: tt.paths}
			if got := reporter.StoragePaths(); !reflect.DeepEqual(got, tt.paths) {
				t.Errorf("StoragePaths() = %v, want %v", got, tt.paths)
			}
		})
	}
}
