package pool

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHealthStateTracksAndEvictsUnhealthyDNSAbsences(t *testing.T) {
	state := NewHealthState()
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, state.Track([]string{"10.0.0.1", "10.0.0.2"}))
	state.Update(
		map[string]bool{"10.0.0.1": true, "10.0.0.2": true},
		nil,
		[]string{"10.0.0.1", "10.0.0.2"},
		2,
		nil,
	)

	state.Update(
		map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		nil,
		[]string{"10.0.0.1"},
		2,
		nil,
	)
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, state.Resolved())
	state.Update(
		map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		nil,
		[]string{"10.0.0.1"},
		2,
		nil,
	)

	assert.ElementsMatch(t, []string{"10.0.0.1"}, state.Resolved())
	assert.False(t, state.IsHealthy("10.0.0.2"))
}

func TestHealthStateRetainsLastKnownHealthyDuringDiscoveryAndHealthFailure(t *testing.T) {
	state := NewHealthState()
	state.Track([]string{"10.0.0.1"})
	state.Update(
		map[string]bool{"10.0.0.1": true},
		nil,
		[]string{"10.0.0.1"},
		2,
		nil,
	)

	for range 3 {
		state.Update(
			map[string]bool{"10.0.0.1": false},
			nil,
			nil,
			2,
			nil,
		)
	}

	assert.ElementsMatch(t, []string{"10.0.0.1"}, state.Resolved())
	assert.False(t, state.IsHealthy("10.0.0.1"))
}

func TestHealthStateEvictsFormerHealthyAfterNewHealthySetAppears(t *testing.T) {
	state := NewHealthState()
	state.Track([]string{"10.0.0.1", "10.0.0.2"})
	state.Update(
		map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		nil,
		[]string{"10.0.0.1", "10.0.0.2"},
		2,
		nil,
	)

	for range 2 {
		state.Update(
			map[string]bool{"10.0.0.1": false, "10.0.0.2": true},
			nil,
			[]string{"10.0.0.2"},
			2,
			nil,
		)
	}

	assert.ElementsMatch(t, []string{"10.0.0.2"}, state.Resolved())
	assert.False(t, state.IsHealthy("10.0.0.1"))
	assert.True(t, state.IsHealthy("10.0.0.2"))
}

func TestHealthStateDepartedAddressFromAnotherCluster(t *testing.T) {
	const departed, current = "10.0.0.1", "10.0.0.2"
	const ours, theirs = "sysid-ours", "sysid-theirs"
	tests := []struct {
		name         string
		departedID   string
		currentID    string
		wantHealthy  bool
		wantRetained bool
	}{
		{name: "another cluster's pod on a departed IP gets no traffic and is evicted", departedID: theirs, currentID: ours, wantHealthy: false, wantRetained: false},
		{name: "a terminating member of the same cluster keeps its protection", departedID: ours, currentID: ours, wantHealthy: true, wantRetained: true},
		{name: "no decision without an identifier from an address in DNS", departedID: theirs, currentID: "", wantHealthy: true, wantRetained: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := NewHealthState()
			state.Track([]string{departed, current})
			for range 2 {
				state.Update(
					map[string]bool{departed: true, current: true},
					map[string]string{departed: tt.departedID, current: tt.currentID},
					[]string{current},
					2,
					nil,
				)
			}
			assert.Equal(t, tt.wantHealthy, state.IsHealthy(departed))
			assert.Equal(t, tt.wantRetained, slices.Contains(state.Resolved(), departed))
		})
	}
}
