package tracker

// Changes conf changes
type Changes struct {
	Added   []uint64
	Removed []uint64
}

// Clone clone ProgressTracker
func (p *ProgressTracker) Clone() ProgressTracker {
	value := ProgressTracker{Votes: make(map[uint64]bool, len(p.Voters)), Progress: make(ProgressMap, len(p.Progress))}
	value.Config = p.Config
	value.MaxInflight = p.MaxInflight

	for id, v := range p.Voters {
		value.Voters[id] = v
	}

	for id, pr := range p.Progress {
		p := *pr
		p.Inflights = pr.Inflights.Clone()
		value.Progress[id] = &p
	}

	return value
}

// ApplyConf Applies configuration and updates progress map to match the configuration.
func (p *ProgressTracker) ApplyConf(conf Config, changes *Changes, nextIdx uint64) {
	p.Config = conf
	for _, id := range changes.Added {
		pr := &Progress{
			Match:                  0,
			Next:                   nextIdx,
			State:                  StateProbe,
			ProbeSent:              false,
			PendingSnapshot:        0,
			PendingRequestSnapshot: 0,
			RecentActive:           false,
			Inflights:              NewInflights(p.MaxInflight),
			CommitGroupID:          0,
			CommittedIndex:         0,
		}

		// When a node is first added, we should mark it as recently active.
		// Otherwise, CheckQuorum may cause us to step down if it is invoked
		// before the added node has had a chance to communicate with us.
		pr.RecentActive = true
		p.Progress[id] = pr
	}

	for _, id := range changes.Removed {
		delete(p.Progress, id)
	}
}
