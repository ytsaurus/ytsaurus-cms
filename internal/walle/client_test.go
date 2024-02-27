package walle

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

const testWalleProject = Project("yp-iss-man-yt-freud")

func TestClient_existingHost(t *testing.T) {
	testWalleClient := os.Getenv("TEST_WALLE_CLIENT")
	t.Logf("This test talks to %s.", walleURL)
	if testWalleClient == "" {
		t.Skip("TEST_WALLE_CLIENT environment variable is not set")
	}

	ctx := context.Background()
	client := NewClient()

	host, err := findHost(ctx, testWalleProject)
	require.NoError(t, err)
	t.Logf("host: %s", host)

	project, err := client.FindProject(ctx, host)
	require.NoError(t, err)

	require.Equal(t, testWalleProject, project)
}

// findHost returns some host belonging to the project.
func findHost(ctx context.Context, project Project) (string, error) {
	type response struct {
		Result []*struct {
			Name string `json:"name"`
		} `json:"result"`
	}

	queryURL := fmt.Sprintf("%s/v1/hosts", walleURL)
	resp, err := resty.New().R().
		SetContext(ctx).
		SetQueryParams(map[string]string{
			"project": string(project),
			"fields":  "name",
		}).
		SetQueryParam("fields", "name,project").
		SetResult(&response{}).
		Get(queryURL)

	if err != nil {
		return "", err
	}

	if resp.StatusCode() != http.StatusOK {
		return "", xerrors.Errorf("bad status code: body=%s", resp.String())
	}

	result := resp.Result().(*response).Result
	if len(result) == 0 {
		return "", xerrors.New("no hosts found for the project")
	}

	return result[0].Name, nil
}

func TestClient_nonExistingHost(t *testing.T) {
	testWalleClient := os.Getenv("TEST_WALLE_CLIENT")
	t.Logf("This test talks to %s.", walleURL)
	if testWalleClient == "" {
		t.Skip("TEST_WALLE_CLIENT environment variable is not set")
	}

	ctx := context.Background()
	client := NewClient()

	_, err := client.FindProject(ctx, "not-a-host-at-all")
	require.Error(t, err)
	require.Equal(t, err, ErrHostNotFound)
}

func TestClient_GetHostInfo(t *testing.T) {
	testWalleClient := os.Getenv("TEST_WALLE_CLIENT")
	t.Logf("This test talks to %s.", walleURL)
	if testWalleClient == "" {
		t.Skip("TEST_WALLE_CLIENT environment variable is not set")
	}

	ctx := context.Background()
	client := NewClient()

	host, err := findHost(ctx, testWalleProject)
	require.NoError(t, err)
	t.Logf("got host: %q", host)

	m, err := client.GetHostInfo(ctx, host, "not-a-host-at-all")
	require.NoError(t, err)

	require.Contains(t, m, host)
	require.Equal(t, host, m[host].Name)
	require.NotContains(t, m, "not-a-host-at-all")
}
