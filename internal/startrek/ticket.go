package startrek

import (
	"strings"
	"text/template"

	"go.ytsaurus.tech/yt/go/yson"
)

// TicketKey is a string that holds ticket key.
type TicketKey string

// Ticket is a startrek ticket request.
type Ticket struct {
	Assignee    *Entity   `json:"assignee,omitempty"`
	Followers   []*Entity `json:"followers,omitempty"`
	Description string    `json:"description,omitempty"`
	Queue       *Queue    `json:"queue,omitempty"`
	Summary     string    `json:"summary,omitempty"`
}

// NewTicketResponse is a ticket creation response.
type NewTicketResponse struct {
	Key TicketKey `json:"key"`
}

// confirmMasterTicketTemplate is a template for a startrek ticket about manual master confirmation.
const confirmMasterTicketTemplate = `Сервис ((https://wiki.yandex-team.ru/wall-e/ Wall-e)) запрашивает разрешение на обслуживание серверов кластера ((https://yt.yandex-team.ru/{{.Proxy}}/system {{.Proxy}})).
По данным системы **GOCMS** на оборудовании присутствует компонента %%{{.ClusterRole}}%% , автоматическая обработка которой запрещена.

**Идентификатор заявки**: %%{{.ID}}%%
**Тип заявки**: {{.Type}}
**Планируемое действие**: {{.Action}}
**Инициатор**: {{.Issuer}}
**Комментарий**: {{.Comment}}

**Список оборудования**:
{{range $i, $h := .HostStates -}}
  * ((https://wall-e.yandex-team.ru/?hosts={{ $i }} {{ $i }}))
  {{range $addr, $r := $h.Roles -}}
    * %%{{ printf "%-18s" $r.Type }} {{ printf "%s" $addr.MarshalText }}%%
  {{end}}
{{end}}

<{Состояние заявки
%%(sh)
yt select-rows "* from [//sys/cms/tasks] where id = '{{.ID}}'" --proxy={{.Proxy}} --format="<format=pretty>yson"
%%
%%
{{ marshal .Task }}
%%
}>

Отменить задачу можно в Wall-e.

**Что делать?**

Прежде чем выводить хост убедитесь, что все ((https://wiki.yandex-team.ru/yt/internal/duty/manual/#zajavkanaobsluzhivanieoborudovanijahostxxx необходимые подготовительные действия)) произведены!
{{if .EnableFollowerProcessing}}
На кластере включена автоматическая отдача фолловеров. Достаточно переключить лидера.
%%
ya tool yt execute switch_leader '{cell_id="{{.CellID}}"; new_leader_address="FQDN:PORT"}' --proxy={{.Proxy}}
%%
<{Как подтвердить заявку руками?
Находясь в корне аркадии:

1. Соберите клиент:
  %%(sh)
ya make yt/admin/cms/cmd/cms-cli
%%
2. Подтвердите заявку:
  %%(sh)
./yt/admin/cms/cmd/cms-cli/cms-cli confirm --host={{StringsJoin .Hosts ","}} --proxy={{.Proxy}}
%%
}>
{{else}}
Находясь в корне аркадии:

1. Соберите клиент:
  %%(sh)
ya make yt/admin/cms/cmd/cms-cli
%%
2. Подтвердите заявку:
  %%(sh)
./yt/admin/cms/cmd/cms-cli/cms-cli confirm --host={{StringsJoin .Hosts ","}} --proxy={{.Proxy}}
%%
{{- end -}}
`

var ConfirmMasterTicketTemplate = template.Must(template.New("config").
	Funcs(map[string]any{
		"marshal": func(v any) string {
			data, _ := yson.MarshalFormat(v, yson.FormatPretty)
			return string(data)
		},
		"StringsJoin": strings.Join,
	}).Parse(confirmMasterTicketTemplate))

// MustExecuteTemplate applies parsed template to specified data object.
//
// Panics in case of an error.
func MustExecuteTemplate(t *template.Template, data any) string {
	builder := strings.Builder{}
	if err := t.Execute(&builder, data); err != nil {
		panic(err)
	}
	return builder.String()
}

type Transition string

const (
	TransitionStartProgress Transition = "start_progress"
	TransitionClose         Transition = "close"
)

// Entity represents a person or a robot.
type Entity struct {
	// Login is a staff login.
	Login string `json:"id,omitempty"`
}

// Queue represents startrek queue.
type Queue struct {
	Key string `json:"key,omitempty"`
}

// TicketRelationship is a separate type to store ticket relationships.
type TicketRelationship string

const (
	// RelationRelates is the simplest possible relationship
	// meaning that tickets are somehow related.
	RelationRelates TicketRelationship = "relates"
)
