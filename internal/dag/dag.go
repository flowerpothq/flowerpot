package dag

import "fmt"

// Graph represents a directed acyclic graph as an adjacency list.
// Keys are node names; values are the nodes they depend on (i.e., must run after).
type Graph map[string][]string

// TopoSort returns a topologically sorted list of node names using Kahn's algorithm.
// If the graph contains a cycle, it returns an error naming the involved nodes.
func TopoSort(g Graph) ([]string, error) {
	// Collect all nodes (some may only appear as dependencies).
	nodes := make(map[string]struct{})
	for name, deps := range g {
		nodes[name] = struct{}{}
		for _, d := range deps {
			nodes[d] = struct{}{}
		}
	}

	// Build forward adjacency (dep -> dependents) and in-degree counts.
	inDegree := make(map[string]int, len(nodes))
	forward := make(map[string][]string, len(nodes))
	for n := range nodes {
		inDegree[n] = 0
	}
	for name, deps := range g {
		inDegree[name] += 0 // ensure entry exists
		for _, dep := range deps {
			forward[dep] = append(forward[dep], name)
			inDegree[name]++
		}
	}

	// Seed the queue with nodes that have zero in-degree.
	// Use a sorted insert to make output deterministic.
	queue := make([]string, 0, len(nodes))
	for n := range nodes {
		if inDegree[n] == 0 {
			queue = insertSorted(queue, n)
		}
	}

	var order []string
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		for _, dependent := range forward[n] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = insertSorted(queue, dependent)
			}
		}
	}

	if len(order) != len(nodes) {
		cycle := findCycleNodes(g, inDegree)
		return nil, fmt.Errorf("cycle detected involving pipelines: %v", cycle)
	}

	return order, nil
}

// findCycleNodes returns the names of nodes still in the cycle (in-degree > 0).
func findCycleNodes(g Graph, inDegree map[string]int) []string {
	var cycleNodes []string
	for n, deg := range inDegree {
		if deg > 0 {
			cycleNodes = insertSorted(cycleNodes, n)
		}
	}
	return cycleNodes
}

// insertSorted inserts s into a sorted slice, maintaining order.
func insertSorted(sorted []string, s string) []string {
	i := 0
	for i < len(sorted) && sorted[i] < s {
		i++
	}
	sorted = append(sorted, "")
	copy(sorted[i+1:], sorted[i:])
	sorted[i] = s
	return sorted
}
