// Copyright 2018 Sorint.lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"errors"
	"testing"
)

func TestValidateReplicationSlots(t *testing.T) {
	tests := []struct {
		in  string
		err error
	}{
		{
			in: "goodslotname_434432",
		},
		{
			in:  "badslotname-34223",
			err: errors.New(`wrong replication slot name: "badslotname-34223"`),
		},
		{
			in:  "badslotname\n",
			err: errors.New(`wrong replication slot name: "badslotname\n"`),
		},
		{
			in:  " badslotname",
			err: errors.New(`wrong replication slot name: " badslotname"`),
		},
		{
			in:  "badslotname ",
			err: errors.New(`wrong replication slot name: "badslotname "`),
		},
		{
			in:  "stolon_c874a3cb",
			err: errors.New(`replication slot name is reserved: "stolon_c874a3cb"`),
		},
	}

	for i, tt := range tests {
		err := validateReplicationSlot(tt.in)

		if tt.err != nil {
			if err == nil {
				t.Errorf("#%d: got no error, wanted error: %v", i, tt.err)
			} else if tt.err.Error() != err.Error() {
				t.Errorf("#%d: got error: %v, wanted error: %v", i, err, tt.err)
			}
		} else {
			if err != nil {
				t.Errorf("#%d: unexpected error: %v", i, err)
			}
		}
	}
}

func TestClusterSpec_Validate(t *testing.T) {
	t.Run("should validate PGBasebackupMaxRate", func(t *testing.T) {
		var initMode = ClusterInitMode("new")
		var spec *ClusterSpec

		tests := []struct {
			rate        string
			shouldError bool
		}{
			{"10", true},
			{"10k", true},
			{"32k", false},
			{"10M", false},
			{"1024M", false},
			{"1023M", false},
			{"1025M", true},
			{"fM", true},
			{"", false},
		}
		for i, tt := range tests {
			spec = &ClusterSpec{
				InitMode:            &initMode,
				PGBasebackupMaxRate: tt.rate,
			}

			err := spec.Validate()

			if tt.shouldError && (err == nil) {
				t.Errorf("#%d: expected error for max-rate: %s, actual no error", i, tt.rate)
			}

			if !tt.shouldError && (err != nil) {
				t.Errorf("#%d: unexpected error for max-rate: %s, error: %v", i, tt.rate, err)
			}
		}
	})
}
