package startrek_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/admin/cms/internal/models"
	"go.ytsaurus.tech/yt/admin/cms/internal/startrek"
	"go.ytsaurus.tech/yt/admin/cms/internal/walle"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytsys"
)

const (
	prodURL   = "https://st-api.yandex-team.ru/v2"
	testQueue = "AHTAIXA"
)

func TestClient_success(t *testing.T) {
	token := os.Getenv("STARTREK_OAUTH_TOKEN")
	t.Logf("This test talks to %s.", prodURL)
	if token == "" {
		t.Skip("STARTREK_OAUTH_TOKEN environment variable is not set")
	}

	client := startrek.NewClient(&startrek.ClientConfig{
		OAuthToken: token,
		URL:        prodURL,
	})

	cellID, err := guid.ParseString("889611cb-ee874187-3f40259-fc6adeeb")
	require.NoError(t, err)

	task := &models.Task{
		Task: &walle.Task{
			ID:      "production-3184915",
			Type:    "AUTOMATED",
			Action:  "PROFILE",
			Issuer:  "wall-e",
			Comment: "CPU overheat detected: Critical temperature for coretemp.1: Core 2: 85.0C>=85.0C, Core 1: 86.0C>=85.0C, Core 0: 85.0C>=85.0C, Physical id 1: 86.0C>=85.0C..",
			Hosts:   []string{"man1-6063.search.yandex.net"},
		},
		HostStates: map[string]*models.Host{
			"man1-6063.search.yandex.net": {
				Host: "man1-6063.search.yandex.net",
				Roles: map[ypath.Path]*models.Component{
					"//sys/primary_masters/m033-freud.man.yp-c.yandex.net:9010": {
						Type: ytsys.RolePrimaryMaster,
						Role: &models.Master{
							Host:   "man1-6063.search.yandex.net",
							Addr:   &ytsys.Addr{FQDN: "m033-freud.man.yp-c.yandex.net", Port: "9010"},
							CellID: yt.NodeID(cellID),
						},
					},
					"//sys/primary_masters/man1-6063-node-freud.man.yp-c.yandex.net:9012": {
						Type: ytsys.RoleNode,
						Role: &models.Node{
							Host: "man1-6063.search.yandex.net",
							Addr: &ytsys.Addr{FQDN: "man1-6063-node-freud.man.yp-c.yandex.net", Port: "9012"},
						},
					},
				},
			},
		},
	}

	description := startrek.MustExecuteTemplate(startrek.ConfirmMasterTicketTemplate, struct {
		*models.Task
		Proxy                    string
		EnableFollowerProcessing bool
		CellID                   yt.NodeID
		ClusterRole              ytsys.ClusterRole
	}{Task: task, Proxy: "freud", EnableFollowerProcessing: true, CellID: yt.NodeID(cellID), ClusterRole: ytsys.RolePrimaryMaster})

	ticket := &startrek.Ticket{
		Description: description,
		Queue:       &startrek.Queue{Key: testQueue},
		Summary:     "Заявка на обслуживание оборудования кластера freud: man1-6063.search.yandex.net (primary_master)",
	}

	ctx := context.Background()
	key, err := client.CreateTicket(ctx, ticket)
	require.NoError(t, err)
	require.NotEmpty(t, key)
	t.Logf("created ticket %s", key)

	require.NoError(t, client.StartProgress(ctx, key, "robot-yt-cms"))

	relatedKey, err := client.CreateTicket(ctx, ticket)
	require.NoError(t, err)
	require.NotEmpty(t, key)
	t.Logf("created ticket %s", relatedKey)

	require.NoError(t, client.LinkTicket(ctx, key, relatedKey, startrek.RelationRelates))

	err = client.LinkTicket(ctx, key, relatedKey, startrek.RelationRelates)
	require.Error(t, err)
	require.True(t, xerrors.Is(err, startrek.ErrAlreadyRelated))

	require.NoError(t, client.CloseTicket(ctx, relatedKey))
	require.NoError(t, client.CloseTicket(ctx, key))
}
