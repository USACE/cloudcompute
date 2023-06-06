package cloudcompute

import (
	"fmt"
	"testing"
)

func TestTS4(t *testing.T) {
	digraph := map[string][]string{
		"B": []string{"A"},
		"C": []string{"B"},
		"D": []string{"C"},
	}
	sorted, err := TopologicalSort(digraph)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(sorted)
}

func TestTS3(t *testing.T) {
	digraph := map[string][]string{
		"C": []string{"D"},
		"B": []string{"C"},
		"A": []string{"B"},
	}
	sorted, err := TopologicalSort(digraph)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(sorted)
}

func TestTS2(t *testing.T) {
	digraph := map[string][]string{
		"5": []string{"2", "0"},
		"4": []string{"0", "1"},
		"2": []string{"3"},
		"3": []string{"1"},
	}
	sorted, err := TopologicalSort(digraph)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(sorted)
}

func TestTS(t *testing.T) {
	digraph := map[string][]string{
		"1": []string{"2", "4"},
		"2": []string{"3", "5"},
		"3": []string{"4", "5"},
	}
	sorted, err := TopologicalSort(digraph)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(sorted)
}

//node() //connectedTo()

func TestTS5(t *testing.T) {
	digraph := map[string][]string{
		"1": []string{},
		"2": []string{"1"},
		"3": []string{"2"},
		"4": []string{"1", "3"},
		"5": []string{"2", "3"},
	}
	sorted, err := TopologicalSort(digraph)
	if err != nil {
		t.Log(err)
	}
	fmt.Println(sorted)
}

func TestTopoSort(t *testing.T) {
	manifests := []ComputeManifest{
		{
			ManifestID:   "1",
			Dependencies: []JobDependency{},
		},
		{
			ManifestID:   "2",
			Dependencies: []JobDependency{{"1"}},
		},
		{
			ManifestID:   "3",
			Dependencies: []JobDependency{{"2"}},
		},
		{
			ManifestID:   "4",
			Dependencies: []JobDependency{{"1"}, {"3"}},
		},
		{
			ManifestID:   "5",
			Dependencies: []JobDependency{{"2"}, {"3"}},
		},
	}
	event := Event{
		Manifests: manifests,
	}
	ordered, err := event.TopoSort()
	if err != nil {
		t.Log(err)
	}
	fmt.Println(ordered)
}
