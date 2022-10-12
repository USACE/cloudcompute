package watcompute

import (
	"errors"
)

//Interface for supporting Topological sort in Manifests (or other structs that would use a toposort)
type TopoSortable[T comparable] interface {
	Node() T
	Deps() []T
}

//Manifest Node sort function for string IDs
func (m Manifest) Node() string {
	return m.ManifestID
}

//Manifest Deps sort function for a slice of string dependencies
func (m Manifest) Deps() []string {
	deps := []string{}
	for _, d := range m.Dependencies {
		deps = append(deps, d.JobId)
	}
	return deps
}

//Topological Sort function for a WAT Event
//returns an ordered list of manifest IDs
func (e *Event) TopoSort() ([]string, error) {
	digraph := depsToGraph(e.toTopoSortable())
	return TopologicalSort(digraph)
}

func (e *Event) toTopoSortable() []TopoSortable[string] {
	a := []TopoSortable[string]{}
	for _, v := range e.Manifests {
		a = append(a, v)
	}
	return a
}

//Converts nodal dependency relationships into a dependncy graph that can be used for a topological sort.
//works with any type that implements 'comparable'
func depsToGraph[T comparable](data []TopoSortable[T]) map[T][]T {
	digraph := make(map[T][]T)
	for _, m := range data {
		for _, d := range m.Deps() {
			if _, ok := digraph[d]; ok {
				digraph[d] = append(digraph[d], m.Node())
			} else {
				digraph[d] = []T{m.Node()}
			}
		}
	}
	return digraph
}

//Generic topological sort function.
//supports all types that implement 'comparable'
func TopologicalSort[T comparable](digraph map[T][]T) ([]T, error) {
	indegrees := make(map[T]int)
	for u := range digraph {
		if digraph[u] != nil {
			for _, v := range digraph[u] {
				indegrees[v]++
			}
		}
	}

	var queue []T
	for u := range digraph {
		if _, ok := indegrees[u]; !ok {
			queue = append(queue, u)
		}
	}

	var order []T
	for len(queue) > 0 {
		u := queue[len(queue)-1]
		queue = queue[:(len(queue) - 1)]
		order = append(order, u)
		for _, v := range digraph[u] {
			indegrees[v]--
			if indegrees[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	for _, indegree := range indegrees {
		if indegree > 0 {
			return order, errors.New("not a DAG")
		}
	}
	return order, nil
}
