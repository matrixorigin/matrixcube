// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package placement

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func makeTestStores() StoreSet {
	containers := core.NewCachedStores()
	for zone := 1; zone <= 5; zone++ {
		for rack := 1; rack <= 5; rack++ {
			for host := 1; host <= 5; host++ {
				for x := 1; x <= 5; x++ {
					id := uint64(zone*1000 + rack*100 + host*10 + x)
					labels := map[string]string{
						"zone": fmt.Sprintf("zone%d", zone),
						"rack": fmt.Sprintf("rack%d", rack),
						"host": fmt.Sprintf("host%d", host),
						"id":   fmt.Sprintf("id%d", x),
					}
					containers.SetStore(core.NewTestStoreInfoWithLabel(id, 0, labels))
				}
			}
		}
	}
	return containers
}

// example: "1111_leader,1234,2111_learner"
func makeTestShard(def string) *core.CachedShard {
	var resourceMeta metapb.Shard
	var leader *metapb.Replica
	for _, peerDef := range strings.Split(def, ",") {
		role, idStr := Follower, peerDef
		if strings.Contains(peerDef, "_") {
			splits := strings.Split(peerDef, "_")
			idStr, role = splits[0], ReplicaRoleType(splits[1])
		}
		id, _ := strconv.Atoi(idStr)
		peer := metapb.Replica{ID: uint64(id), StoreID: uint64(id), Role: role.MetaPeerRole()}
		resourceMeta.SetReplicas(append(resourceMeta.GetReplicas(), peer))
		if role == Leader {
			leader = &peer
		}
	}
	return core.NewCachedShard(&resourceMeta, leader)
}

// example: "3/voter/zone=zone1+zone2,rack=rack2/zone,rack,host"
//       count role constraints location_labels
func makeTestRule(def string) *Rule {
	var rule Rule
	splits := strings.Split(def, "/")
	rule.Count, _ = strconv.Atoi(splits[0])
	rule.Role = ReplicaRoleType(splits[1])
	// only support k=v type constraint
	for _, c := range strings.Split(splits[2], ",") {
		if c == "" {
			break
		}
		kv := strings.Split(c, "=")
		rule.LabelConstraints = append(rule.LabelConstraints, LabelConstraint{
			Key:    kv[0],
			Op:     "in",
			Values: strings.Split(kv[1], "+"),
		})
	}
	rule.LocationLabels = strings.Split(splits[3], ",")
	return &rule
}

func checkPeerMatch(peers []metapb.Replica, expect string) bool {
	if len(peers) == 0 && expect == "" {
		return true
	}

	m := make(map[string]struct{})
	for _, p := range peers {
		m[strconv.Itoa(int(p.ID))] = struct{}{}
	}
	expects := strings.Split(expect, ",")
	if len(expects) != len(m) {
		return false
	}
	for _, p := range expects {
		delete(m, p)
	}
	return len(m) == 0
}

func TestFitresource(t *testing.T) {
	containers := makeTestStores()

	cases := []struct {
		resource string
		rules    []string
		fitPeers string
	}{
		// test count
		{"1111,1112,1113", []string{"1/voter//"}, "1111"},
		{"1111,1112,1113", []string{"2/voter//"}, "1111,1112"},
		{"1111,1112,1113", []string{"3/voter//"}, "1111,1112,1113"},
		{"1111,1112,1113", []string{"5/voter//"}, "1111,1112,1113"},
		// best location
		{"1111,1112,1113,2111,2222,3222,3333", []string{"3/voter//zone,rack,host"}, "1111,2111,3222"},
		{"1111,1121,1211,2111,2211", []string{"3/voter//zone,rack,host"}, "1111,1211,2111"},
		{"1111,1211,1311,1411,2111,2211,2311,3111", []string{"5/voter//zone,rack,host"}, "1111,1211,2111,2211,3111"},
		// test role match
		{"1111_learner,1112,1113", []string{"1/voter//"}, "1112"},
		{"1111_learner,1112,1113", []string{"2/voter//"}, "1112,1113"},
		{"1111_learner,1112,1113", []string{"3/voter//"}, "1111,1112,1113"},
		{"1111,1112_learner,1121_learner,1122_learner,1131_learner,1132,1141,1142", []string{"3/follower//zone,rack,host"}, "1111,1132,1141"},
		// test 2 rule
		{"1111,1112,1113,1114", []string{"3/voter//", "1/voter/id=id1/"}, "1112,1113,1114/1111"},
		{"1111,2211,3111,3112", []string{"3/voter//zone", "1/voter/rack=rack2/"}, "1111,2211,3111//3112"},
		{"1111,2211,3111,3112", []string{"1/voter/rack=rack2/", "3/voter//zone"}, "2211/1111,3111,3112"},
	}

	for _, cc := range cases {
		resource := makeTestShard(cc.resource)
		var rules []*Rule
		for _, r := range cc.rules {
			rules = append(rules, makeTestRule(r))
		}
		rf := FitShard(containers, resource, rules)
		expects := strings.Split(cc.fitPeers, "/")
		for i, f := range rf.RuleFits {
			assert.True(t, checkPeerMatch(f.Peers, expects[i]))
		}
		if len(rf.RuleFits) < len(expects) {
			assert.True(t, checkPeerMatch(rf.OrphanPeers, expects[len(rf.RuleFits)]))
		}
	}
}

func TestIsolationScore(t *testing.T) {
	containers := makeTestStores()
	testCases := []struct {
		peers1 []uint64
		op     string
		peers2 []uint64
	}{
		{[]uint64{1111, 1112}, "<", []uint64{1111, 1121}},
		{[]uint64{1111, 1211}, "<", []uint64{1111, 2111}},
		{[]uint64{1111, 1211, 1311, 2111, 3111}, "<", []uint64{1111, 1211, 2111, 2211, 3111}},
		{[]uint64{1111, 1211, 2111, 2211, 3111}, "==", []uint64{1111, 2111, 2211, 3111, 3211}},
		{[]uint64{1111, 1211, 2111, 2211, 3111}, ">", []uint64{1111, 1121, 2111, 2211, 3111}},
	}

	makePeers := func(ids []uint64) []*fitPeer {
		var peers []*fitPeer
		for _, id := range ids {
			peers = append(peers, &fitPeer{
				Replica:   metapb.Replica{StoreID: id},
				container: containers.GetStore(id),
			})
		}
		return peers
	}

	for _, tc := range testCases {
		peers1, peers2 := makePeers(tc.peers1), makePeers(tc.peers2)
		score1 := isolationScore(peers1, []string{"zone", "rack", "host"})
		score2 := isolationScore(peers2, []string{"zone", "rack", "host"})
		switch tc.op {
		case "<":
			assert.True(t, score1 < score2)
		case ">":
			assert.True(t, score1 > score2)
		case "==":
			assert.True(t, score1 == score2)
		}
	}
}
