package coredata

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

type sessionFact map[string]json.RawMessage

// Projection is a capability boundary. Unknown/private fields are discarded,
// including nested parameters/checkpoints, rather than forwarded as raw rows.
func publicSessionFact(raw json.RawMessage, fields string) (sessionFact, error) {
	if !object(raw) {
		return nil, failure("data_loss", "stored Session graph fact is not an object")
	}
	var source sessionFact
	if err := json.Unmarshal(raw, &source); err != nil {
		return nil, failure("data_loss", "invalid stored Session graph fact")
	}
	out := sessionFact{}
	for key, value := range source {
		field, ok := sessionPublicFieldSets[fields][strings.ToLower(key)]
		if !ok {
			continue
		}
		if _, duplicate := out[field]; duplicate {
			return nil, failure("data_loss", "ambiguous stored public field")
		}
		scalar := bytes.TrimSpace(value)
		if len(scalar) == 0 || scalar[0] == '{' || scalar[0] == '[' {
			return nil, failure("data_loss", "stored public field is not scalar")
		}
		if string(scalar) != "null" && (strings.HasSuffix(field, "At") || field == "deadline") {
			var timestamp time.Time
			if json.Unmarshal(value, &timestamp) != nil {
				return nil, failure("data_loss", "invalid public fact timestamp")
			}
		}
		if field == "targetRoot" {
			var flag bool
			if json.Unmarshal(value, &flag) != nil || string(scalar) == "null" {
				return nil, failure("data_loss", "invalid child target-root flag")
			}
		}
		out[field] = value
	}
	return out, nil
}

func (f sessionFact) text(key string) string { var v string; _ = json.Unmarshal(f[key], &v); return v }
func (f sessionFact) integer(key string) int64 {
	v, _ := strconv.ParseInt(string(f[key]), 10, 64)
	return v
}
func (f sessionFact) boolean(key string) bool       { return string(f[key]) == "true" }
func (f sessionFact) present(key string) bool       { return len(f[key]) != 0 && string(f[key]) != "null" }
func (f sessionFact) raw() json.RawMessage          { b, _ := encode(f); return b }
func storedIdentity(f sessionFact, key string) bool { return textKey(f.text(key)) }
func storedLifecycle(f sessionFact) bool            { return storedIdentity(f, "id") && f.integer("revision") > 0 }

func canonicalSessionPin(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

const sessionPublicFields = "id targetId experimentResourceId experimentCommitId experimentDigest robotSelectionDigest state mode runMode revision error createdAt updatedAt activatedAt stoppingAt completedAt"
const memberPublicFields = "id targetId sessionId bindingId kind ownerId status revision error createdAt updatedAt completedAt artifactPath artifactStartedAt artifactEndedAt"
const runPublicFields = "id targetId automationResourceId definitionId definitionVersion configDigest executionPlanDigest registryDigest definitionDigest actionId actionVersion executionModel throughNodeId parentRunId rootRunId callNodeId depth correlationId status admissionMode admissionScope admissionLimit admissionOnConflict replacesRunId terminationKind reason acceptedAt createdAt startedAt updatedAt finishedAt revision"

// activeAttemptId is a minimal authenticated snapshot ownership pointer, not a
// public Core NodeInvocation field. occurrences validates it before returning.
const invocationPublicFields = "id runId nodeId kind status activeAttemptId currentWaitId waitGeneration nextAttemptAt startedAt finishedAt createdAt updatedAt revision"
const attemptPublicFields = "id runId invocationId phase number status adoptionCount createdAt startedAt finishedAt updatedAt revision"
const originPublicFields = "RunTargetID RunID RootRunID DefinitionID ConfigDigest ExecutionPlanDigest RegistryDigest DefinitionDigest NodeID NodeKind NodeTypeVersion InvocationID"
const jobPublicFields = "ID TargetID Kind Status Revision AttemptCount UpdatedAt"
const jobAttemptPublicFields = "ID Number Status StartedAt FinishedAt"

var relationPublicFields = map[string]string{
	"childRuns":            "id targetId rootRunId parentRunId parentInvocationId callNodeId ordinal childRunId ownerRunId childDefinitionId childDefinitionVersion childConfigDigest childExecutionPlanDigest childRegistryDigest childDefinitionDigest triggerNodeId targetRoot targetRootBindingId targetRootPresetId targetRootActionId targetRootActionVersion targetRootRunMode observedStatus observedRevision observedAt observedFinishedAt observedReason runStatus runRevision relation waitPolicy cancelPolicy resultPolicy createdAt updatedAt boundAt launchAbandonedAt launchAbandonedReason revision",
	"childRunGroups":       "id targetId rootRunId parentRunId producerInvocationId producerNodeId groupKey expectedMembers memberCount membershipDigest waitPolicy joinMode failurePolicy remainingPolicy resultPolicy maxConcurrency state outcome winnerChildRunId terminalCount createdAt updatedAt sealedAt resolvedAt revision",
	"childRunGroupMembers": "id groupId ordinal itemKey childRunId state createdAt updatedAt dispatchedAt terminalAt revision",
	"waits":                "id generation type subjectId runId invocationId attemptId state wakeAt deadline reason terminalAt deliveredAt createdAt updatedAt revision",
	"effects":              "id targetId runId invocationId preparedAttemptId effectKey kind ownership checkpointDigest state externalIdentity primaryErrorClass primaryError compensationPolicy compensationState compensationAttemptCount compensationErrorClass compensationError preparedAt applyingAt primaryTerminalAt compensationStartedAt compensationFinishedAt updatedAt revision",
	"runtimeGroups":        "id targetId runId invocationId preparedAttemptId groupKey manifestDigest state lifecycleError createdAt updatedAt terminalAt revision",
	"runtimes":             "id targetId groupId runId invocationId bindingKey backendKind backendId ownership relation cleanupPolicy ownerType ownerId state createdAt updatedAt transferredAt releaseStartedAt releasedAt revision",
	"resources":            "id targetId runId invocationId boundAttemptId runtimeGroupId bindingKey resourceKey mode capacity slot ownership cleanupPolicy scope scopeId ownerType ownerId state createdAt updatedAt releasedAt lostAt lossReason cleanupError revision",
}

// Immutable lookup tables are private to this compiled projection. Each fact
// makes one pass over its fields; unknown private bytes are never decoded.
var sessionPublicFieldSets = func() map[string]map[string]string {
	lists := []string{sessionPublicFields, memberPublicFields, runPublicFields, invocationPublicFields, attemptPublicFields, originPublicFields, jobPublicFields, jobAttemptPublicFields}
	for _, fields := range relationPublicFields {
		lists = append(lists, fields)
	}
	out := map[string]map[string]string{}
	for _, fields := range lists {
		set := map[string]string{}
		for _, field := range strings.Fields(fields) {
			set[strings.ToLower(field)] = field
		}
		out[fields] = set
	}
	return out
}()
