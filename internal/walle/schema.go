// Package walle contains data types and JSON schema describing CMS API v1.4.
//
// Package also provides functions to validate JSON requests and responses.
//
// For details see https://wiki.yandex-team.ru/wall-e/guide/cms/v1.3/.
package walle

import (
	"encoding/json"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"golang.org/x/xerrors"
)

// ErrUnknownName indicates that validation encountered an unknown schema name.
var ErrUnknownName = xerrors.New("unknown walle schema name")

const (
	// PostTaskRequestSchema - schema name corresponding to postTaskRequestSchema.
	PostTaskRequestSchema = "post_task_request"
	// GetTaskResponseSchema - schema name corresponding to getTaskResponseSchema.
	GetTaskResponseSchema = "get_task_response"
	// GetTasksResponseSchema - schema name corresponding to getTasksResponseSchema.
	GetTasksResponseSchema = "get_tasks_response"
)

func init() {
	registerSchema(PostTaskRequestSchema, postTaskRequestSchema)
	registerSchema(GetTaskResponseSchema, getTaskResponseSchema)
	registerSchema(GetTasksResponseSchema, getTasksResponseSchema)
}

// JSON schema by name (string->*jsonschema.Schema).
var schemas sync.Map

// registerSchema loads schema from string and inserts it into package-level map.
func registerSchema(name, contents string) {
	s, err := jsonschema.CompileString(name, contents)
	if err != nil {
		panic(xerrors.Errorf("unable to load json schema %s: %w", name, err))
	}
	schemas.Store(name, s)
}

// Check validates json data against schema.
//
// Returns ErrUnknownName error when unable to find schema by name.
func Check(name string, data []byte) error {
	schema, ok := schemas.Load(name)
	if !ok {
		return ErrUnknownName
	}

	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return xerrors.Errorf("cannot unmarshal data as JSON: %w", err)
	}

	err := schema.(*jsonschema.Schema).Validate(v)
	if err != nil {
		return xerrors.Errorf("failed to validate JSON against schema: %w", err)
	}
	return nil
}

// postTaskRequestSchema is a json schema corresponding to POST /tasks http request.
//
// https://wiki.yandex-team.ru/wall-e/guide/cms/v1.3/#post/tasks
const postTaskRequestSchema = `{
    "title": "Add new task to CMS",
    "description": "Wall-E creates a new task for CMS, giving it a list of hosts and a task he wants to perform.",
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
            "minLength": 1,
            "maxLength": 255,
            "description": "Task ID"
        },
        "type": {
            "enum": ["manual", "automated"],
            "description": "Task type"
        },
        "issuer": {
            "type": "string",
            "minLength": 1,
            "description": "Action issuer"
        },
        "action": {
            "enum": ["prepare", "deactivate", "power-off", "reboot", "profile", "redeploy", "repair-link", "change-disk", "temporary-unreachable"],
            "description": "Requested action"
        },
        "hosts": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "string",
                "minLength": 1
            },
            "description": "Hosts to process the action on"
        },
        "comment": {
            "type": "string",
            "description": "optional comment from task's author"
        },
        "extra": {
            "type": "object",
            "description": "additional task parameters"
        }
    },
    "required": ["id", "type", "issuer", "action", "hosts"],
    "additionalProperties": true
}`

// getTaskResponseSchema is a json schema corresponding to GET /tasks/<task_id> http response.
//
// https://wiki.yandex-team.ru/wall-e/guide/cms/v1.3/#get/tasks/taskid
const getTaskResponseSchema = `{
    "title": "Task as it was created by CMS",
    "description": "Wall-E checks task that were created earlier. If no task was found, CMS should return 404 NOT FOUND.",
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
            "minLength": 1,
            "maxLength": 255,
            "description": "Task ID must match requested task ID"
        },
        "type": {
            "enum": ["manual", "automated"],
            "description": "Task type"
        },
        "issuer": {
            "type": "string",
            "minLength": 1,
            "description": "Action issuer"
        },
        "action": {
            "enum": ["prepare", "deactivate", "power-off", "reboot", "profile", "redeploy", "repair-link", "change-disk", "temporary-unreachable"],
            "description": "Requested action"
        },
        "hosts": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "string",
                "minLength": 1
            },
            "description": "Hosts to process the action on"
        },
        "comment": {
            "type": "string",
            "description": "optional comment from task's author"
        },
        "extra": {
            "type": "object",
            "description": "optional task parameters"
        },
        "status": {
            "enum": ["ok", "in-process", "rejected"],
            "description": "Current status of CMS task"
        },
        "message": {
            "type": "string",
            "description": "Message for the current status"
        }
    },
    "required": ["id", "hosts", "status"]
}`

// getTasksResponseSchema is a json schema corresponding to GET /tasks http response.
//
// https://wiki.yandex-team.ru/wall-e/guide/cms/v1.3/#get/tasks
const getTasksResponseSchema = `{
    "title": "List of current tasks",
    "description": "Wall-E looks for staled tasks. CMS should return an object, containing a (possibly empty) list of tasks.",
    "type": "object",
    "properties": {
        "result": {
            "type": "array",
            "items": {
                "title": "Task as it was created by CMS",
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 255,
                        "description": "Task ID must match requested task ID"
                    },
                    "type": {
                        "enum": ["manual", "automated"],
                        "description": "Task type"
                    },
                    "issuer": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Action issuer"
                    },
                    "action": {
                        "enum": ["prepare", "deactivate", "power-off", "reboot", "profile", "redeploy", "repair-link", "change-disk", "temporary-unreachable"],
                        "description": "Requested action"
                    },
                    "hosts": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "string",
                            "minLength": 1
                        },
                        "description": "Hosts to process the action on"
                    },
                    "comment": {
                        "type": "string",
                        "description": "optional comment from task's author"
                    },
                    "extra": {
                        "type": "object",
                        "description": "optional task parameters"
                    },
                    "status": {
                        "enum": ["ok", "in-process", "rejected"],
                        "description": "Current status of CMS task"
                    },
                    "message": {
                        "type": "string",
                        "description": "Message for the current status"
                    }
                },
                "required": ["id", "hosts", "status"]
            }
        }
    },
    "required": ["result"]
}
`
