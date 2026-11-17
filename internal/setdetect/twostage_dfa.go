// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package setdetect

// TwoStageDFA runs the first stage as an Aho-Corasick automaton over the
// case-folded patterns "set", "exec", "u&" and ";": one table lookup per
// byte, no branches on the pattern set.
type TwoStageDFA struct{}

// Name implements Detector.
func (TwoStageDFA) Name() string { return "two-stage-dfa" }

// Classify implements Detector.
func (TwoStageDFA) Classify(sql string, scsOn bool) Verdict {
	if !stage1DFA(sql) {
		return verdict(leadingDoCall(sql))
	}
	return verdict(lexDirty(sql, scsOn))
}

// dfa[state][byte] is the next state; state dfaAccept means a match.
var dfa, dfaAccept = buildDFA([]string{"set", "exec", "u&", ";"})

func stage1DFA(sql string) bool {
	s := uint8(0)
	for i := 0; i < len(sql); i++ {
		s = dfa[s][sql[i]]
		if s == dfaAccept {
			return true
		}
	}
	return false
}

func buildDFA(patterns []string) ([][256]uint8, uint8) {
	// Trie of the patterns; state 0 is the root.
	type node struct {
		next   map[byte]int
		fail   int
		accept bool
	}
	nodes := []node{{next: map[byte]int{}}}
	for _, p := range patterns {
		s := 0
		for i := 0; i < len(p); i++ {
			c := p[i]
			t, ok := nodes[s].next[c]
			if !ok {
				nodes = append(nodes, node{next: map[byte]int{}})
				t = len(nodes) - 1
				nodes[s].next[c] = t
			}
			s = t
		}
		nodes[s].accept = true
	}
	// Breadth-first failure links.
	queue := []int{}
	for _, t := range nodes[0].next {
		queue = append(queue, t)
	}
	for len(queue) > 0 {
		s := queue[0]
		queue = queue[1:]
		for c, t := range nodes[s].next {
			f := nodes[s].fail
			for {
				if u, ok := nodes[f].next[c]; ok && u != t {
					nodes[t].fail = u
					break
				}
				if f == 0 {
					nodes[t].fail = 0
					break
				}
				f = nodes[f].fail
			}
			nodes[t].accept = nodes[t].accept || nodes[nodes[t].fail].accept
			queue = append(queue, t)
		}
	}
	accept := uint8(len(nodes))
	table := make([][256]uint8, len(nodes)+1)
	for s := range nodes {
		for b := 0; b < 256; b++ {
			c := byte(b)
			if c >= 'A' && c <= 'Z' {
				c += 32
			}
			f := s
			for {
				if t, ok := nodes[f].next[c]; ok {
					if nodes[t].accept {
						table[s][b] = accept
					} else {
						table[s][b] = uint8(t)
					}
					break
				}
				if f == 0 {
					table[s][b] = 0
					break
				}
				f = nodes[f].fail
			}
		}
	}
	for b := range table[accept] {
		table[accept][b] = accept
	}
	return table, accept
}
