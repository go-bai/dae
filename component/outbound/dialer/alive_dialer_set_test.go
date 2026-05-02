/*
 * SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (c) 2022-2025, daeuniverse Organization <dae@v2raya.org>
 */

package dialer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/daeuniverse/dae/common/consts"
)

func TestAliveDialerSet_GetRandExcludedConcurrent(t *testing.T) {
	networkType := newTestNetworkType()
	dialers := []*Dialer{
		newNamedTestDialer(t, "dialer-1"),
		newNamedTestDialer(t, "dialer-2"),
		newNamedTestDialer(t, "dialer-3"),
	}

	set := NewAliveDialerSet(
		dialers[0].Log,
		"test-group",
		networkType,
		0,
		0,
		1,
		consts.DialerSelectionPolicy_Random,
		dialers,
		[]*Annotation{{}, {}, {}},
		func(bool) {},
		true,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	excluded := dialers[0]
	errCh := make(chan error, 32)
	var wg sync.WaitGroup

	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				selected := set.GetRandExcluded(excluded)
				if selected == nil {
					errCh <- fmt.Errorf("GetRandExcluded returned nil")
					return
				}
				if selected == excluded {
					errCh <- fmt.Errorf("GetRandExcluded returned the excluded dialer")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
}

// TestAliveDialerSet_SwitchCooldownSuppressesVoluntarySwitch verifies that a
// dialer with a better latency is not promoted to current-best while the switch
// cooldown is still active.
func TestAliveDialerSet_SwitchCooldownSuppressesVoluntarySwitch(t *testing.T) {
	networkType := newTestNetworkType()
	d0 := newNamedTestDialer(t, "dialer-0")
	d1 := newNamedTestDialer(t, "dialer-1")
	dialers := []*Dialer{d0, d1}

	set := NewAliveDialerSet(
		d0.Log,
		"test-group",
		networkType,
		0,
		time.Hour, // long cooldown
		1,
		consts.DialerSelectionPolicy_MinMovingAverageLatencies,
		dialers,
		[]*Annotation{{}, {}},
		func(bool) {},
		false,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	// Set d0 with 100ms moving average and bring it alive first.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 100 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	// Set d1 with a better 50ms moving average, but it arrives while cooldown is active.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 50 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	// The cooldown (1h) must suppress the voluntary switch to d1.
	selected, _ := set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("expected d0 to remain selected during cooldown, got another dialer")
	}
}

// TestAliveDialerSet_SwitchMinWinsRequiresConsecutiveWins verifies that a
// candidate dialer must beat the current best for switchMinWins consecutive
// notifications before a voluntary switch is allowed.
func TestAliveDialerSet_SwitchMinWinsRequiresConsecutiveWins(t *testing.T) {
	networkType := newTestNetworkType()
	d0 := newNamedTestDialer(t, "dialer-0")
	d1 := newNamedTestDialer(t, "dialer-1")
	d2 := newNamedTestDialer(t, "dialer-2")
	dialers := []*Dialer{d0, d1, d2}

	set := NewAliveDialerSet(
		d0.Log,
		"test-group",
		networkType,
		0,
		0, // no cooldown
		2, // switchMinWins=2: need 2 consecutive wins
		consts.DialerSelectionPolicy_MinMovingAverageLatencies,
		dialers,
		[]*Annotation{{}, {}, {}},
		func(bool) {},
		false,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	// Establish d0 as current best at 100ms.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 100 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	selected, _ := set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("initial: expected d0 selected, got %v", selected)
	}

	// First notification: d1 beats d0 at 50ms — should NOT switch yet (wins=1 < 2).
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 50 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after 1st d1 win: expected d0 still selected, got %v", selected)
	}

	// Second notification: d1 beats d0 at 40ms — should switch now (wins=2 >= 2).
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 40 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d1 {
		t.Fatalf("after 2nd d1 win: expected d1 selected, got %v", selected)
	}

	// Now d2 appears at 30ms. First notification: should NOT switch (wins reset to 1).
	d2.collectionFineMu.Lock()
	d2.mustGetCollection(networkType).MovingAverage = 30 * time.Millisecond
	d2.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d2, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d1 {
		t.Fatalf("after 1st d2 win (new candidate): expected d1 still selected, got %v", selected)
	}
}

func TestAliveDialerSet_SwitchMinWinsResetsWhenCandidateStopsWinning(t *testing.T) {
	networkType := newTestNetworkType()
	d0 := newNamedTestDialer(t, "dialer-0")
	d1 := newNamedTestDialer(t, "dialer-1")
	dialers := []*Dialer{d0, d1}

	set := NewAliveDialerSet(
		d0.Log,
		"test-group",
		networkType,
		0,
		0, // no cooldown
		2, // switchMinWins=2: need 2 consecutive wins
		consts.DialerSelectionPolicy_MinMovingAverageLatencies,
		dialers,
		[]*Annotation{{}, {}},
		func(bool) {},
		false,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	// Establish d0 as current best at 100ms.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 100 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	selected, _ := set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("initial: expected d0 selected, got %v", selected)
	}

	// d1 wins once at 50ms; switchMinWins=2 means d0 remains selected.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 50 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after first d1 win: expected d0 still selected, got %v", selected)
	}

	// d1 no longer beats d0; this must reset d1's consecutive-win state.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 150 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after d1 stops winning: expected d0 still selected, got %v", selected)
	}

	// d1 wins again, but this is the first win after reset and must not switch yet.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 40 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after first d1 win post-reset: expected d0 still selected, got %v", selected)
	}

	// Second consecutive win after reset should promote d1.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 30 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d1 {
		t.Fatalf("after second d1 win post-reset: expected d1 selected, got %v", selected)
	}
}

func TestAliveDialerSet_SwitchMinWinsResetsWhenCurrentBestImproves(t *testing.T) {
	networkType := newTestNetworkType()
	d0 := newNamedTestDialer(t, "dialer-0")
	d1 := newNamedTestDialer(t, "dialer-1")
	dialers := []*Dialer{d0, d1}

	set := NewAliveDialerSet(
		d0.Log,
		"test-group",
		networkType,
		0,
		0, // no cooldown
		2, // switchMinWins=2: need 2 consecutive wins
		consts.DialerSelectionPolicy_MinMovingAverageLatencies,
		dialers,
		[]*Annotation{{}, {}},
		func(bool) {},
		false,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	// Establish d0 as current best at 100ms.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 100 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	selected, _ := set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("initial: expected d0 selected, got %v", selected)
	}

	// d1 wins once at 50ms; d0 remains selected.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 50 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after first d1 win: expected d0 still selected, got %v", selected)
	}

	// d0 improves to 40ms, so d1's cached 50ms no longer wins and its streak must reset.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 40 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after d0 improves: expected d0 still selected, got %v", selected)
	}

	// d1 wins again at 35ms, but this is the first win after reset and must not switch yet.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 35 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after first d1 win post-reset: expected d0 still selected, got %v", selected)
	}

	// Second consecutive win after reset should promote d1.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 30 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d1 {
		t.Fatalf("after second d1 win post-reset: expected d1 selected, got %v", selected)
	}
}

func TestAliveDialerSet_SwitchMinWinsAppliesWhenCurrentBestWorsens(t *testing.T) {
	networkType := newTestNetworkType()
	d0 := newNamedTestDialer(t, "dialer-0")
	d1 := newNamedTestDialer(t, "dialer-1")
	dialers := []*Dialer{d0, d1}

	set := NewAliveDialerSet(
		d0.Log,
		"test-group",
		networkType,
		0,
		0, // no cooldown
		2, // switchMinWins=2: need 2 consecutive wins
		consts.DialerSelectionPolicy_MinMovingAverageLatencies,
		dialers,
		[]*Annotation{{}, {}},
		func(bool) {},
		false,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	// Establish d0 as current best at 100ms.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 100 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	// d1 is alive but initially worse than d0.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 150 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ := set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("initial: expected d0 selected, got %v", selected)
	}

	// d0 worsens, making d1 the better candidate. This is a voluntary switch and
	// should still require consecutive wins instead of bypassing switchMinWins via
	// a full min-latency recalculation.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 200 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("after first d1 win caused by d0 worsening: expected d0 still selected, got %v", selected)
	}

	// Another d0 latency update gives d1 its second consecutive win.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 250 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	selected, _ = set.GetMinLatency(nil)
	if selected != d1 {
		t.Fatalf("after second d1 win caused by d0 worsening: expected d1 selected, got %v", selected)
	}
}

// TestAliveDialerSet_SwitchGatesBypassedWhenCurrentBestDies verifies that when
// the current best dialer dies, the next best is selected immediately regardless
// of cooldown or switchMinWins settings.
func TestAliveDialerSet_SwitchGatesBypassedWhenCurrentBestDies(t *testing.T) {
	networkType := newTestNetworkType()
	d0 := newNamedTestDialer(t, "dialer-0")
	d1 := newNamedTestDialer(t, "dialer-1")
	dialers := []*Dialer{d0, d1}

	set := NewAliveDialerSet(
		d0.Log,
		"test-group",
		networkType,
		0,
		time.Hour, // long cooldown
		100,       // very high switchMinWins — voluntary switch should never fire
		consts.DialerSelectionPolicy_MinMovingAverageLatencies,
		dialers,
		[]*Annotation{{}, {}},
		func(bool) {},
		false,
	)
	for _, d := range dialers {
		d.RegisterAliveDialerSet(set)
	}
	t.Cleanup(func() {
		for _, d := range dialers {
			d.UnregisterAliveDialerSet(set)
		}
	})

	// Establish d0 as current best at 100ms.
	d0.collectionFineMu.Lock()
	d0.mustGetCollection(networkType).MovingAverage = 100 * time.Millisecond
	d0.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d0, true)

	// d1 is alive at 50ms — voluntary switch is suppressed by cooldown+minWins.
	d1.collectionFineMu.Lock()
	d1.mustGetCollection(networkType).MovingAverage = 50 * time.Millisecond
	d1.collectionFineMu.Unlock()
	set.NotifyLatencyChange(d1, true)

	selected, _ := set.GetMinLatency(nil)
	if selected != d0 {
		t.Fatalf("before d0 dies: expected d0 selected, got %v", selected)
	}

	// d0 dies — failover must select d1 immediately, bypassing all gates.
	set.NotifyLatencyChange(d0, false)

	selected, _ = set.GetMinLatency(nil)
	if selected != d1 {
		t.Fatalf("after d0 dies: expected d1 selected immediately, got %v", selected)
	}
}
