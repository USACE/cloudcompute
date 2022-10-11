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

/*


type manifestStack []Manifest

func (ms *manifestStack) Push(m Manifest) {
	*ms = append(*ms, m)
}

func (ms *manifestStack) Pop() (Manifest, error) {
	if len(*ms) == 0 {
		return Manifest{}, errors.New("no more elements in the stack")
	}
	id := len(*ms) - 1
	lm := (*ms)[id]
	*ms = (*ms)[:id]
	return lm, nil
}

func TopologicallySort(manifests []Manifest) ([]Manifest, error) {
	//Kahn's Algorithm https://en.wikipedia.org/wiki/Topological_sorting
	S := manifestStack{} //set of linked manifests with no upstream dependencies
	L := manifestStack{}

	for _, lm := range manifests {
		noDependencies := true
		for _, input := range lm.Inputs {
			_, ok := dag.producesFile(input)
			if ok {
				noDependencies = false
			} else {
				if len(input.InternalPaths) > 0 {
					for _, ip := range input.InternalPaths {
						_, _, ipok := dag.producesInternalPath(ip)
						if ipok {
							noDependencies = false
						}
					}
				}
			}
		}
		if noDependencies {
			S.Push(lm)
		}
	}

	if len(S) == 0 {
		return S, errors.New("a DAG must contain at least one node with no dependencies satisfied by other linked manifests in the DAG")
	}

	for len(S) > 0 {
		n, err := S.Pop()
		if err != nil {
			return S, err
		}

		L.Push(n)
		for _, m := range dag.LinkedManifests {
			noOtherDependencies := true
			for _, input := range m.Inputs {
				_, dagok := dag.producesFile(input)
				if dagok {
					inL := false
					//should i check for anything in L?
					//_, ok := n.producesFile(input.SourceDataId)
					for _, Ln := range L {
						_, ok := Ln.producesFile(input.SourceDataId)
						if ok {
							inL = true
						}
					}
					if !inL {
						noOtherDependencies = false
					}
				} else {
					if len(input.InternalPaths) > 0 {
						for _, ip := range input.InternalPaths {
							_, _, ipok := dag.producesInternalPath(ip)
							if ipok {
								inL := false
								//should i check for anything in L?
								for _, Ln := range L {
									_, _, ok := Ln.producesInternalPath(ip)
									if ok {
										inL = true
									}
								}
								if !inL {
									noOtherDependencies = false
								}
							}
						}
					}
				}
			}
			if noOtherDependencies {
				visited := false
				for _, visitedNode := range L {
					if m.ManifestID == visitedNode.ManifestID {
						visited = true
					}
				}
				for _, addedToS := range S {
					if m.ManifestID == addedToS.ManifestID {
						visited = true // added to s but not yet popped off the stack
					}
				}
				if !visited {
					S.Push(m)
				}
			}
		}
	}
	if len(L) != len(dag.LinkedManifests) {
		return L, errors.New("the DAG contains a sub-cycle") //we could identify it by listing the elements in the dag that are not present in the Stack L
	}
	return L, nil
}

func (lm Manifest) producesFile(fileId string) (plugin.FileData, bool) {
	for _, output := range lm.Outputs {
		if fileId == output.Id {
			return output, true
		}
	}
	return plugin.FileData{}, false
}

func (lf Manifest) producesInternalPath(internalPath LinkedInternalPathData) (string, string, bool) {
	output, ok := lf.producesFile(internalPath.SourceFileID)
	if ok {
		if len(output.InternalPaths) > 0 {
			for _, ip := range output.InternalPaths {
				if internalPath.SourcePathID == ip.Id {
					return ip.PathName, output.FileName, true
				}
			}
		}
		return "", output.FileName, true
	}
	return "", "", false
}

func (lm Manifest) producesDependency(linkedFile LinkedFileData) bool {
	for _, output := range lm.Outputs {
		if linkedFile.SourceDataId == output.Id {
			return true
		}
		if linkedFile.HasInternalPaths() {
			for _, internalPath := range linkedFile.InternalPaths {
				if internalPath.SourceFileID == output.Id {
					return true
				}
			}
		}
	}
	return false
}
*/
