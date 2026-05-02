/*
 * SPDX-License-Identifier: AGPL-3.0-only
 * Copyright (c) 2022-2025, daeuniverse Organization <dae@v2raya.org>
 */

package dialer

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/daeuniverse/dae/common/consts"
	"github.com/daeuniverse/outbound/pkg/fastrand"
	"github.com/sirupsen/logrus"
)

const (
	Init = 1 + iota
	NotAlive
)

type minLatency struct {
	sortingLatency time.Duration
	dialer         *Dialer
}

// aliveEntry combines a dialer pointer with its cached sorting latency.
// This struct enables slice-based storage that eliminates map lookups in hot paths.
type aliveEntry struct {
	dialer         *Dialer
	sortingLatency time.Duration
}

// AliveDialerSet assumes mapping between index and dialer MUST remain unchanged.
//
// It is thread-safe.
type AliveDialerSet struct {
	log             *logrus.Logger
	dialerGroupName string
	CheckTyp        *NetworkType
	tolerance       time.Duration
	switchCooldown  time.Duration
	switchMinWins   int

	aliveChangeCallback func(alive bool)

	mu                    sync.RWMutex
	dialerToIndex         map[*Dialer]int // *Dialer -> index in aliveEntries, -Init, or -NotAlive
	dialerToLatency       map[*Dialer]time.Duration
	dialerToLatencyOffset map[*Dialer]time.Duration

	// aliveEntries stores all alive dialers with their precomputed sorting latency.
	// This is the primary data structure for hot path operations (GetMinLatency, GetRandExcluded).
	// Using a slice of structs provides better cache locality and eliminates map lookups.
	aliveEntries []aliveEntry

	selectionPolicy consts.DialerSelectionPolicy
	minLatency      minLatency

	// Switch stability state (guarded by mu).
	lastSwitchAt      time.Time
	candidateDialer   *Dialer
	candidateWins     int
	lastCandidateWins int
}

func NewAliveDialerSet(
	log *logrus.Logger,
	dialerGroupName string,
	networkType *NetworkType,
	tolerance time.Duration,
	switchCooldown time.Duration,
	switchMinWins int,
	selectionPolicy consts.DialerSelectionPolicy,
	dialers []*Dialer,
	dialersAnnotations []*Annotation,
	aliveChangeCallback func(alive bool),
	setAlive bool,
) *AliveDialerSet {
	if len(dialers) != len(dialersAnnotations) {
		panic(fmt.Sprintf("unmatched annotations length: %v dialers and %v annotations", len(dialers), len(dialersAnnotations)))
	}
	dialerToLatencyOffset := make(map[*Dialer]time.Duration)
	for i := range dialers {
		d, a := dialers[i], dialersAnnotations[i]
		dialerToLatencyOffset[d] = a.AddLatency
	}
	a := &AliveDialerSet{
		log:                   log,
		dialerGroupName:       dialerGroupName,
		CheckTyp:              networkType,
		tolerance:             tolerance,
		switchCooldown:        switchCooldown,
		switchMinWins:         switchMinWins,
		aliveChangeCallback:   aliveChangeCallback,
		dialerToIndex:         make(map[*Dialer]int),
		dialerToLatency:       make(map[*Dialer]time.Duration),
		dialerToLatencyOffset: dialerToLatencyOffset,
		aliveEntries:          make([]aliveEntry, 0, len(dialers)),
		selectionPolicy:       selectionPolicy,
		minLatency: minLatency{
			// Initiate the latency with a very big value.
			sortingLatency: time.Hour,
		},
	}
	for _, d := range dialers {
		a.dialerToIndex[d] = -Init
	}
	for _, d := range dialers {
		a.NotifyLatencyChange(d, setAlive)
	}
	return a
}

func (a *AliveDialerSet) GetRand() *Dialer {
	return a.GetRandExcluded(nil)
}

func (a *AliveDialerSet) GetRandExcluded(excluded *Dialer) *Dialer {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.aliveEntries) == 0 {
		return nil
	}
	if excluded == nil {
		return a.aliveEntries[fastrand.Intn(len(a.aliveEntries))].dialer
	}

	var chosen *Dialer
	var candidateCount int
	for i := range a.aliveEntries {
		d := a.aliveEntries[i].dialer
		if d == excluded {
			continue
		}
		candidateCount++
		// Reservoir sampling keeps uniform randomness without a shared scratch buffer.
		if fastrand.Intn(candidateCount) == 0 {
			chosen = d
		}
	}

	return chosen
}

func (a *AliveDialerSet) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.aliveEntries)
}

func (a *AliveDialerSet) SwitchCooldown() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.switchCooldown
}

func (a *AliveDialerSet) SwitchMinWins() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.switchMinWins
}

// cooldownAllowsSwitchLocked reports whether the switch cooldown has elapsed.
// Must be called with a.mu held (read or write).
func (a *AliveDialerSet) cooldownAllowsSwitchLocked() bool {
	if a.switchCooldown <= 0 || a.lastSwitchAt.IsZero() {
		return true
	}
	return time.Since(a.lastSwitchAt) >= a.switchCooldown
}

// resetSwitchCandidateLocked clears the consecutive-wins candidate state.
// Must be called with a.mu held for writing.
func (a *AliveDialerSet) resetSwitchCandidateLocked() {
	a.candidateDialer = nil
	a.candidateWins = 0
}

// candidateBeatsCurrentBestLocked reports whether candidateSortingLatency qualifies
// as a winning challenger against the current best using the same tolerance
// predicate as voluntary promotion.
// Must be called with a.mu held (read or write).
func (a *AliveDialerSet) candidateBeatsCurrentBestLocked(candidate *Dialer, candidateSortingLatency time.Duration) bool {
	return candidate != nil &&
		a.minLatency.dialer != nil &&
		candidate != a.minLatency.dialer &&
		candidateSortingLatency <= a.minLatency.sortingLatency &&
		(a.minLatency.sortingLatency < a.tolerance || candidateSortingLatency <= a.minLatency.sortingLatency-a.tolerance)
}

// resetSwitchCandidateIfNotWinningLocked clears candidate state if the cached
// candidate no longer beats the current best after a current-best latency change.
// Must be called with a.mu held for writing.
func (a *AliveDialerSet) resetSwitchCandidateIfNotWinningLocked() {
	if a.candidateDialer == nil {
		return
	}
	candidateLatency, ok := a.dialerToLatency[a.candidateDialer]
	if !ok {
		a.resetSwitchCandidateLocked()
		return
	}
	candidateSortingLatency := candidateLatency + a.dialerToLatencyOffset[a.candidateDialer]
	if !a.candidateBeatsCurrentBestLocked(a.candidateDialer, candidateSortingLatency) {
		a.resetSwitchCandidateLocked()
	}
}

// canPromoteCandidateLocked decides whether a voluntary switch to the given
// candidate dialer with the given sortingLatency should be allowed.
// It gates on the switch cooldown and the consecutive-wins requirement.
// Returns (allowed, wins) where wins is the current candidateWins count.
// Must be called with a.mu held for writing.
func (a *AliveDialerSet) canPromoteCandidateLocked(candidate *Dialer, sortingLatency time.Duration) (bool, int) {
	// If there is no current best (or the candidate is the current best), allow freely.
	if a.minLatency.dialer == nil || a.minLatency.dialer == candidate {
		return true, 0
	}

	// Apply cooldown gate first; if cooldown suppresses, log and return.
	if !a.cooldownAllowsSwitchLocked() {
		remaining := a.switchCooldown - time.Since(a.lastSwitchAt)
		if a.log.IsLevelEnabled(logrus.DebugLevel) {
			a.log.WithFields(logrus.Fields{
				"group":              a.dialerGroupName,
				"network":            a.CheckTyp.String(),
				"candidate_dialer":   candidate.property.Name,
				"current_dialer":     a.minLatency.dialer.property.Name,
				"candidate_latency":  sortingLatency,
				"current_latency":    a.minLatency.sortingLatency,
				"switch_cooldown":    a.switchCooldown,
				"cooldown_remaining": remaining,
			}).Debugln("Dialer switch suppressed by cooldown")
		}
		return false, 0
	}

	// If switchMinWins <= 1, no consecutive-wins gate required.
	if a.switchMinWins <= 1 {
		return true, 1
	}

	// Track consecutive wins for the same candidate.
	if a.candidateDialer != candidate {
		a.candidateDialer = candidate
		a.candidateWins = 0
	}
	a.candidateWins++

	if a.candidateWins < a.switchMinWins {
		if a.log.IsLevelEnabled(logrus.DebugLevel) {
			a.log.WithFields(logrus.Fields{
				"group":             a.dialerGroupName,
				"network":           a.CheckTyp.String(),
				"candidate_dialer":  candidate.property.Name,
				"current_dialer":    a.minLatency.dialer.property.Name,
				"candidate_wins":    a.candidateWins,
				"switch_min_wins":   a.switchMinWins,
				"candidate_latency": sortingLatency,
				"current_latency":   a.minLatency.sortingLatency,
			}).Debugln("Dialer switch waiting for consecutive wins")
		}
		return false, a.candidateWins
	}

	return true, a.candidateWins
}

func (a *AliveDialerSet) SortingLatency(d *Dialer) time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if idx, ok := a.dialerToIndex[d]; ok && idx >= 0 && idx < len(a.aliveEntries) {
		return a.aliveEntries[idx].sortingLatency
	}
	// Fallback to direct calculation (should not happen in normal operation).
	return a.dialerToLatency[d] + a.dialerToLatencyOffset[d]
}

func (a *AliveDialerSet) bestAliveDialerExceptLocked(excluded *Dialer) (*Dialer, time.Duration) {
	var bestDialer *Dialer
	bestSortingLatency := time.Hour
	for i := range a.aliveEntries {
		entry := &a.aliveEntries[i]
		if entry.dialer == excluded {
			continue
		}
		if entry.sortingLatency < bestSortingLatency {
			bestSortingLatency = entry.sortingLatency
			bestDialer = entry.dialer
		}
	}
	return bestDialer, bestSortingLatency
}

// GetMinLatency acquires correct selectionPolicy.
func (a *AliveDialerSet) GetMinLatency(excluded *Dialer) (d *Dialer, latency time.Duration) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.minLatency.dialer != nil && excluded != a.minLatency.dialer {
		return a.minLatency.dialer, a.minLatency.sortingLatency
	}

	// Find the best non-excluded dialer.
	// Using aliveEntries with direct field access avoids map lookups.
	nextBest, nextBestSortingLatency := a.bestAliveDialerExceptLocked(excluded)

	if nextBest != nil {
		return nextBest, nextBestSortingLatency
	}

	// No dialer available
	return nil, time.Hour
}

func (a *AliveDialerSet) printLatencies() {
	var builder strings.Builder
	fmt.Fprintf(&builder, "Group '%v' [%v]:\n", a.dialerGroupName, a.CheckTyp.String())
	var alive []*struct {
		d *Dialer
		l time.Duration
		o time.Duration
	}
	for i := range a.aliveEntries {
		d := a.aliveEntries[i].dialer
		latency, ok := a.dialerToLatency[d]
		if !ok {
			continue
		}
		offset := a.dialerToLatencyOffset[d]
		alive = append(alive, &struct {
			d *Dialer
			l time.Duration
			o time.Duration
		}{d, latency, offset})
	}
	sort.SliceStable(alive, func(i, j int) bool {
		return alive[i].l+alive[i].o < alive[j].l+alive[j].o
	})
	for i, dl := range alive {
		fmt.Fprintf(&builder, "%4d. [%v] %v: %v\n", i+1, dl.d.property.SubscriptionTag, dl.d.property.Name, latencyString(dl.l, dl.o))
	}
	a.log.Infoln(strings.TrimSuffix(builder.String(), "\n"))
}

// NotifyLatencyChange should be invoked when dialer every time latency and alive state changes.
func (a *AliveDialerSet) NotifyLatencyChange(dialer *Dialer, alive bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var (
		rawLatency     time.Duration
		sortingLatency time.Duration
		hasLatency     bool
		minPolicy      bool
	)

	switch a.selectionPolicy {
	case consts.DialerSelectionPolicy_MinLastLatency:
		rawLatency, hasLatency = dialer.snapshotLatencyForPolicy(a.CheckTyp, a.selectionPolicy)
		minPolicy = true
	case consts.DialerSelectionPolicy_MinAverage10Latencies:
		rawLatency, hasLatency = dialer.snapshotLatencyForPolicy(a.CheckTyp, a.selectionPolicy)
		minPolicy = true
	case consts.DialerSelectionPolicy_MinMovingAverageLatencies:
		rawLatency, hasLatency = dialer.snapshotLatencyForPolicy(a.CheckTyp, a.selectionPolicy)
		minPolicy = true
	}

	if alive {
		index := a.dialerToIndex[dialer]
		if index >= 0 {
			// This dialer is already alive.
		} else {
			// Dialer: not alive -> alive.
			if index == -NotAlive {
				if a.log.IsLevelEnabled(logrus.InfoLevel) {
					a.log.WithFields(logrus.Fields{
						"dialer": dialer.property.Name,
						"group":  a.dialerGroupName,
					}).Infof("[NOT ALIVE --%v-> ALIVE]", a.CheckTyp.String())
				}
			}
			a.dialerToIndex[dialer] = len(a.aliveEntries)
			a.aliveEntries = append(a.aliveEntries, aliveEntry{
				dialer:         dialer,
				sortingLatency: 0, // Will be updated below if hasLatency
			})
		}
	} else {
		index := a.dialerToIndex[dialer]
		if index >= 0 {
			// Dialer: alive -> not alive.
			if a.log.IsLevelEnabled(logrus.InfoLevel) {
				a.log.WithFields(logrus.Fields{
					"dialer": dialer.property.Name,
					"group":  a.dialerGroupName,
				}).Infof("[ALIVE --%v-> NOT ALIVE]", a.CheckTyp.String())
			}
			// Remove the dialer from aliveEntries.
			if index >= len(a.aliveEntries) {
				a.log.Panicf("index:%v >= len(a.aliveEntries):%v", index, len(a.aliveEntries))
			}
			a.dialerToIndex[dialer] = -NotAlive
			if index < len(a.aliveEntries)-1 {
				// Swap this element with the last element.
				// CRITICAL: Must update dialerToIndex for the swapped dialer.
				lastIdx := len(a.aliveEntries) - 1
				swappedEntry := a.aliveEntries[lastIdx]
				if dialer == swappedEntry.dialer {
					a.log.Panicf("dialer[%p] == swappedEntry.dialer[%p]", dialer, swappedEntry.dialer)
				}

				a.dialerToIndex[swappedEntry.dialer] = index
				a.aliveEntries[index] = swappedEntry
			}
			// Pop the last element.
			a.aliveEntries = a.aliveEntries[:len(a.aliveEntries)-1]
		}
	}

	if hasLatency {
		bakOldBestDialer := a.minLatency.dialer
		bakOldMinSortingLatency := a.minLatency.sortingLatency
		// Calc minLatency.
		a.dialerToLatency[dialer] = rawLatency
		// Update sorting latency in aliveEntries for GetMinLatency hot path optimization.
		sortingLatency = rawLatency + a.dialerToLatencyOffset[dialer]
		// If dialer is alive, update its sortingLatency in aliveEntries.
		if index := a.dialerToIndex[dialer]; index >= 0 {
			a.aliveEntries[index].sortingLatency = sortingLatency
		}
		if alive &&
			(a.minLatency.dialer == nil ||
				a.candidateBeatsCurrentBestLocked(dialer, sortingLatency)) {
			// Voluntary promotion: a non-current dialer is beating the current best.
			// Gate on switch cooldown and consecutive-wins requirement.
			if allowed, wins := a.canPromoteCandidateLocked(dialer, sortingLatency); allowed {
				a.minLatency.sortingLatency = sortingLatency
				a.minLatency.dialer = dialer
				a.lastCandidateWins = wins
				a.resetSwitchCandidateLocked()
			}
			// else: gate suppresses this switch; do not update minLatency.
		} else if dialer == a.candidateDialer {
			// The current candidate no longer qualifies as a winning challenger, so
			// future wins must start a fresh consecutive-wins sequence.
			a.resetSwitchCandidateLocked()
		} else if a.minLatency.dialer == dialer {
			a.minLatency.sortingLatency = sortingLatency
			a.resetSwitchCandidateIfNotWinningLocked()
			if !alive || sortingLatency > bakOldMinSortingLatency {
				// Latency increases.
				if !alive {
					a.minLatency.dialer = nil
					// Current best died; reset candidate so the next contender starts fresh.
					a.resetSwitchCandidateLocked()
					a.calcMinLatency()
					// Now `a.minLatency.dialer` will be nil if there is no alive dialer.
				} else if candidate, candidateSortingLatency := a.bestAliveDialerExceptLocked(dialer); a.candidateBeatsCurrentBestLocked(candidate, candidateSortingLatency) {
					if allowed, wins := a.canPromoteCandidateLocked(candidate, candidateSortingLatency); allowed {
						a.minLatency.sortingLatency = candidateSortingLatency
						a.minLatency.dialer = candidate
						a.lastCandidateWins = wins
						a.resetSwitchCandidateLocked()
					}
				}
			}
		}
		currentAlive := a.minLatency.dialer != nil
		// If best dialer changed.
		if a.minLatency.dialer != bakOldBestDialer {
			if currentAlive {
				// Stamp the switch time so voluntary-switch cooldown begins from
				// every selection change, including failover and recalc-triggered
				// switches that go through calcMinLatency.
				a.lastSwitchAt = time.Now()

				newBestDialer := a.minLatency.dialer
				newBestLatency := a.dialerToLatency[newBestDialer]
				newBestOffset := a.dialerToLatencyOffset[newBestDialer]
				re := "re-"
				var oldDialerName string
				if bakOldBestDialer == nil {
					// Not alive -> alive
					a.mu.Unlock()
					a.aliveChangeCallback(true)
					a.mu.Lock()
					re = ""
					oldDialerName = "<nil>"
				} else {
					oldDialerName = bakOldBestDialer.property.Name
				}
				if a.log.IsLevelEnabled(logrus.InfoLevel) {
					a.log.WithFields(logrus.Fields{
						string(a.selectionPolicy): latencyString(newBestLatency, newBestOffset),
						"_new_dialer":             newBestDialer.property.Name,
						"_old_dialer":             oldDialerName,
						"group":                   a.dialerGroupName,
						"network":                 a.CheckTyp.String(),
						"candidate_wins":          a.lastCandidateWins,
						"switch_min_wins":         a.switchMinWins,
						"switch_cooldown":         a.switchCooldown,
					}).Infof("Group %vselects dialer", re)
				}
				a.lastCandidateWins = 0

				a.printLatencies()
			} else {
				// Alive -> not alive
				a.mu.Unlock()
				a.aliveChangeCallback(false)
				a.mu.Lock()
				if a.log.IsLevelEnabled(logrus.InfoLevel) {
					a.log.WithFields(logrus.Fields{
						"group":   a.dialerGroupName,
						"network": a.CheckTyp.String(),
					}).Infof("Group has no dialer alive")
				}
			}
		}
	} else if alive && minPolicy && a.minLatency.dialer == nil {
		// Use first dialer if no dialer has alive state (usually happen at the very beginning).
		a.minLatency.dialer = dialer
		if a.log.IsLevelEnabled(logrus.InfoLevel) {
			a.log.WithFields(logrus.Fields{
				"group":   a.dialerGroupName,
				"network": a.CheckTyp.String(),
				"dialer":  a.minLatency.dialer.property.Name,
			}).Infof("Group selects dialer")
		}
	}
}

func (a *AliveDialerSet) calcMinLatency() {
	var minLatency = time.Hour
	var minDialer *Dialer
	for i := range a.aliveEntries {
		if a.aliveEntries[i].sortingLatency < minLatency {
			minLatency = a.aliveEntries[i].sortingLatency
			minDialer = a.aliveEntries[i].dialer
		}
	}
	if a.minLatency.dialer == nil {
		a.minLatency.sortingLatency = minLatency
		a.minLatency.dialer = minDialer
	} else if minDialer != nil &&
		minLatency <= a.minLatency.sortingLatency &&
		(a.minLatency.sortingLatency < a.tolerance || minLatency <= a.minLatency.sortingLatency-a.tolerance) {
		a.minLatency.sortingLatency = minLatency
		a.minLatency.dialer = minDialer
	}
}

func (a *AliveDialerSet) SetSelectionPolicy(policy consts.DialerSelectionPolicy) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.selectionPolicy == policy {
		return
	}
	a.selectionPolicy = policy
	a.recomputeSelectionStateLocked()
}

func (a *AliveDialerSet) recomputeSelectionStateLocked() {
	a.dialerToLatency = make(map[*Dialer]time.Duration, len(a.dialerToLatencyOffset))
	a.minLatency = minLatency{
		sortingLatency: time.Hour,
	}
	a.lastSwitchAt = time.Time{}
	a.resetSwitchCandidateLocked()

	if !isMinLatencyPolicy(a.selectionPolicy) {
		return
	}

	for i := range a.aliveEntries {
		entry := &a.aliveEntries[i]
		rawLatency, hasLatency := entry.dialer.snapshotLatencyForPolicy(a.CheckTyp, a.selectionPolicy)
		if hasLatency {
			a.dialerToLatency[entry.dialer] = rawLatency
			entry.sortingLatency = rawLatency + a.dialerToLatencyOffset[entry.dialer]
			continue
		}
		// Keep optimistic startup semantics for alive dialers without latency data yet.
		entry.sortingLatency = 0
	}

	a.calcMinLatency()
}

func isMinLatencyPolicy(policy consts.DialerSelectionPolicy) bool {
	switch policy {
	case consts.DialerSelectionPolicy_MinLastLatency,
		consts.DialerSelectionPolicy_MinAverage10Latencies,
		consts.DialerSelectionPolicy_MinMovingAverageLatencies:
		return true
	default:
		return false
	}
}
