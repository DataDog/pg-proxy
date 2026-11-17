// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/)
// Copyright 2026-Present Datadog, Inc.

package pool

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthStateSnapshotCopiesHealthAndUnprobedAddresses(t *testing.T) {
	const primary, unprobed = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	state.ReplaceRoutingSnapshot(map[string]bool{primary: true}, []string{primary, unprobed})

	snapshot := state.Snapshot()
	require.Equal(t, map[string]bool{primary: true}, snapshot.Health)
	require.ElementsMatch(t, []string{primary, unprobed}, snapshot.Resolved)

	// Modifying either copy must not mutate the source state.
	snapshot.Health[primary] = false
	snapshot.Resolved[0] = "changed"
	current := state.Snapshot()
	require.Equal(t, map[string]bool{primary: true}, current.Health)
	require.ElementsMatch(t, []string{primary, unprobed}, current.Resolved)

	// Replacing the source state must not mutate a previously returned snapshot.
	state.ReplaceRoutingSnapshot(map[string]bool{unprobed: true}, []string{unprobed})
	require.Equal(t, map[string]bool{primary: true}, current.Health)
	require.ElementsMatch(t, []string{primary, unprobed}, current.Resolved)
}

func TestHealthStateSnapshotRemainsConsistentDuringUpdates(t *testing.T) {
	const first, second = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	state.ReplaceRoutingSnapshot(map[string]bool{first: true}, []string{first})
	started, stop, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			state.ReplaceRoutingSnapshot(map[string]bool{second: true}, []string{second})
			state.ReplaceRoutingSnapshot(map[string]bool{first: true}, []string{first})
		}
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	<-started

	for range 1000 {
		snapshot := state.Snapshot()
		require.Len(t, snapshot.Resolved, 1)
		require.Equal(t, map[string]bool{snapshot.Resolved[0]: true}, snapshot.Health)
	}
}

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
	assert.ElementsMatch(t, []string{"10.0.0.1", "10.0.0.2"}, state.Snapshot().Resolved)
	state.Update(
		map[string]bool{"10.0.0.1": true, "10.0.0.2": false},
		nil,
		[]string{"10.0.0.1"},
		2,
		nil,
	)

	assert.ElementsMatch(t, []string{"10.0.0.1"}, state.Snapshot().Resolved)
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

	assert.ElementsMatch(t, []string{"10.0.0.1"}, state.Snapshot().Resolved)
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

	assert.ElementsMatch(t, []string{"10.0.0.2"}, state.Snapshot().Resolved)
	assert.False(t, state.IsHealthy("10.0.0.1"))
	assert.True(t, state.IsHealthy("10.0.0.2"))
}

func TestHealthStateRefreshResolvedRetainsLastKnownHealthyUntilReplacement(t *testing.T) {
	const oldAddress, replacementAddress = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	round := time.Now()
	assert.ElementsMatch(t, []string{oldAddress}, state.RefreshResolved([]string{oldAddress}, 2, nil))
	state.RecordHealthAt(round, map[string]bool{oldAddress: true})

	for range 3 {
		assert.ElementsMatch(t, []string{oldAddress}, state.RefreshResolved(nil, 2, nil))
	}

	assert.ElementsMatch(t, []string{oldAddress, replacementAddress}, state.RefreshResolved([]string{replacementAddress}, 2, nil))
	state.RecordHealthAt(round.Add(time.Second), map[string]bool{oldAddress: false, replacementAddress: true})
	assert.ElementsMatch(t, []string{replacementAddress}, state.RefreshResolved([]string{replacementAddress}, 2, nil))
}

func TestHealthStateRecordHealthAtRestoresEvictedHealthyCandidate(t *testing.T) {
	const oldAddress, replacementAddress = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	oldRound := time.Now()
	replacementRound := oldRound.Add(time.Second)

	assert.ElementsMatch(t, []string{oldAddress}, state.RefreshResolved([]string{oldAddress}, 1, nil))
	state.RecordHealthAt(oldRound, map[string]bool{oldAddress: true})

	// DNS discovers the replacement, but its successful health result remains outstanding.
	assert.ElementsMatch(
		t,
		[]string{oldAddress, replacementAddress},
		state.RefreshResolved([]string{replacementAddress}, 1, nil),
	)

	// The replacement is evicted when DNS becomes empty before its result is recorded. The old
	// address remains protected because it is still the last known healthy candidate.
	assert.ElementsMatch(t, []string{oldAddress}, state.RefreshResolved(nil, 1, nil))

	// Accepting the delayed successful result restores the replacement to the tracked set.
	state.RecordHealthAt(replacementRound, map[string]bool{replacementAddress: true})
	assert.ElementsMatch(t, []string{oldAddress, replacementAddress}, state.Snapshot().Resolved)

	// The next empty-DNS refresh removes the old address but retains the writable replacement.
	assert.ElementsMatch(t, []string{replacementAddress}, state.RefreshResolved(nil, 1, nil))
}

func TestHealthStateRecordHealthAtIgnoresOlderRound(t *testing.T) {
	const oldAddress, replacementAddress = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	state.RefreshResolved([]string{oldAddress, replacementAddress}, 1, nil)
	newerRound := time.Now()

	state.RecordHealthAt(newerRound, map[string]bool{replacementAddress: true})
	state.RecordHealthAt(newerRound.Add(-time.Second), map[string]bool{oldAddress: true})

	assert.ElementsMatch(t, []string{replacementAddress}, state.RefreshResolved(nil, 1, nil))
}

func TestHealthStateRecordHealthAtNewerFailureDoesNotBlockOlderHealthyRound(t *testing.T) {
	const oldAddress, replacementAddress = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	state.RefreshResolved([]string{oldAddress, replacementAddress}, 1, nil)
	oldRound := time.Now()

	state.RecordHealthAt(oldRound.Add(-time.Second), map[string]bool{oldAddress: true})
	state.RecordHealthAt(oldRound.Add(time.Second), map[string]bool{replacementAddress: false})
	state.RecordHealthAt(oldRound, map[string]bool{replacementAddress: true})

	assert.False(t, state.IsHealthy(replacementAddress), "the newer failure remains the current health snapshot")
	assert.ElementsMatch(t, []string{replacementAddress}, state.RefreshResolved(nil, 1, nil))
}

func TestHealthStateRecordHealthAtAcceptsSameRound(t *testing.T) {
	const address = "10.0.0.1"
	state := NewHealthState()
	state.RefreshResolved([]string{address}, 1, nil)
	round := time.Now()

	state.RecordHealthAt(round, map[string]bool{address: false})
	state.RecordHealthAt(round, map[string]bool{address: true})

	assert.ElementsMatch(t, []string{address}, state.RefreshResolved(nil, 1, nil))
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
			assert.Equal(t, tt.wantRetained, slices.Contains(state.Snapshot().Resolved, departed))
		})
	}
}

func TestHealthStateReplaceRoutingSnapshotRefreshesLastKnownHealthy(t *testing.T) {
	const oldAddress, replacementAddress = "10.0.0.1", "10.0.0.2"
	state := NewHealthState()
	state.Update(
		map[string]bool{oldAddress: true, replacementAddress: false},
		nil,
		[]string{oldAddress, replacementAddress},
		1,
		nil,
	)

	state.ReplaceRoutingSnapshot(
		map[string]bool{oldAddress: false, replacementAddress: true},
		[]string{oldAddress, replacementAddress},
	)

	assert.ElementsMatch(t, []string{replacementAddress}, state.RefreshResolved(nil, 1, nil))
}
