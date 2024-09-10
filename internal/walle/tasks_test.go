package walle

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/yt/go/yson"
)

func TestAddTasksRequest_yson(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *AddTaskRequest
	}{
		{
			name: "empty",
			req:  &AddTaskRequest{},
		},
		{
			name: "simple",
			req: &AddTaskRequest{
				ID:      "production-3112557",
				Type:    TaskTypeAutomated,
				Issuer:  "wall-e",
				Action:  ActionChangeDisk,
				Hosts:   []string{"sas29-2490.search.yandex.net"},
				Comment: "testing",
				Extra:   nil,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := yson.Marshal(tc.req)
			require.NoError(t, err)

			var out AddTaskRequest
			err = yson.Unmarshal(data, &out)
			require.NoError(t, err)

			require.Equal(t, tc.req, &out)
		})
	}
}

func TestScenarioInfo_IsNOCScenario(t *testing.T) {
	for _, tc := range []struct {
		typ      ScenarioType
		expected bool
	}{
		{
			typ:      ScenarioTypeHardwareUpgrade,
			expected: false,
		},
		{
			typ:      ScenarioTypeHostTransfer,
			expected: false,
		},
		{
			typ:      ScenarioTypeITDCMaintenance,
			expected: false,
		},
		{
			typ:      ScenarioTypeNOCHard,
			expected: true,
		},
		{
			typ:      ScenarioTypeNOCSoft,
			expected: true,
		},
		{
			typ:      ScenarioTypeNewNOCSoft,
			expected: true,
		},
		{
			typ:      ScenarioType("unknown"),
			expected: false,
		},
	} {
		t.Run(string(tc.typ), func(t *testing.T) {
			i := &ScenarioInfo{Type: tc.typ}
			require.Equal(t, tc.expected, i.IsNOCScenario())
		})
	}
}
