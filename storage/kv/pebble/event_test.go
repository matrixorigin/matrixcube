package pebble

import (
	"testing"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func TestHasEventListener(t *testing.T) {
	assert.False(t, hasEventListener(cpebble.EventListener{}))
	assert.True(t, hasEventListener(cpebble.EventListener{BackgroundError: func(e error) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{CompactionBegin: func(ci cpebble.CompactionInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{CompactionEnd: func(ci cpebble.CompactionInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{DiskSlow: func(dsi cpebble.DiskSlowInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{FlushBegin: func(dsi cpebble.FlushInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{FlushEnd: func(dsi cpebble.FlushInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{ManifestCreated: func(dsi cpebble.ManifestCreateInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{ManifestDeleted: func(dsi cpebble.ManifestDeleteInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{TableCreated: func(dsi cpebble.TableCreateInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{TableDeleted: func(dsi cpebble.TableDeleteInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{TableStatsLoaded: func(dsi cpebble.TableStatsInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{WALCreated: func(dsi cpebble.WALCreateInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{WALDeleted: func(dsi cpebble.WALDeleteInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{WriteStallBegin: func(dsi cpebble.WriteStallBeginInfo) {}}))
	assert.True(t, hasEventListener(cpebble.EventListener{WriteStallEnd: func() {}}))
}
