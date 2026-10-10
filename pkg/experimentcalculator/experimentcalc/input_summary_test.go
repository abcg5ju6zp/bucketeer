// Copyright 2026 The Bucketeer Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package experimentcalc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bucketeer-io/bucketeer/v2/proto/eventcounter"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

func TestBuildGoalInputSummary(t *testing.T) {
	t.Parallel()
	variations := []*featureproto.Variation{
		{Id: "vid1"}, {Id: "vid2"}, {Id: "vid3"},
	}
	evalVc := map[string]*eventcounter.VariationCount{
		"vid1": {VariationId: "vid1", UserCount: 100, EventCount: 105},
		"vid2": {VariationId: "vid2", UserCount: 50, EventCount: 50},
	}
	goalVc := map[string]*eventcounter.VariationCount{
		"vid1": {VariationId: "vid1", UserCount: 20, EventCount: 22, ValueSum: 88.5},
		// vid2 has no conversions; vid3 has no evaluations at all.
	}
	summary := buildGoalInputSummary("goal-id", variations, evalVc, goalVc)
	assert.Equal(t, "goal-id", summary.GoalID)
	assert.Len(t, summary.Variations, 3)
	// snapshot variation order is preserved
	assert.Equal(t, "vid1", summary.Variations[0].VariationID)
	assert.Equal(t, int64(100), summary.Variations[0].EvaluationUserCount)
	assert.Equal(t, int64(105), summary.Variations[0].EvaluationEventCount)
	assert.Equal(t, int64(20), summary.Variations[0].GoalUserCount)
	assert.Equal(t, int64(22), summary.Variations[0].GoalEventCount)
	assert.Equal(t, 88.5, summary.Variations[0].GoalValueSum)
	assert.Equal(t, int64(50), summary.Variations[1].EvaluationUserCount)
	assert.Equal(t, int64(0), summary.Variations[1].GoalUserCount)
	assert.Equal(t, int64(0), summary.Variations[2].EvaluationUserCount)
	assert.Equal(t, int64(0), summary.Variations[2].GoalUserCount)
}

