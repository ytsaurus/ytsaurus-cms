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
