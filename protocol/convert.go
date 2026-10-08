package protocol

import (
	"encoding/json"

	"github.com/XGC-Team/xgc2-storage/api"
)

func NamedInput(r api.NamedRequest) *NamedRequest {
	return &NamedRequest{Scope: scope(r.Scope), DatabaseId: r.DatabaseID, Schema: r.Schema, Module: r.Module, Operation: r.Operation, RequestId: r.RequestID, PayloadJson: r.Payload}
}
func (r *NamedRequest) API() api.NamedRequest {
	return api.NamedRequest{Scope: apiScope(r.Scope), DatabaseID: r.DatabaseId, Schema: r.Schema, Module: r.Module, Operation: r.Operation, RequestID: r.RequestId, Payload: r.PayloadJson}
}
func NamedOutput(r api.NamedResponse) *NamedResponse {
	out := &NamedResponse{ResultJson: r.Result}
	if r.Receipt != nil {
		out.Receipt = ReceiptOutput(*r.Receipt)
	}
	return out
}
func (r *NamedResponse) API() api.NamedResponse {
	out := api.NamedResponse{Result: r.ResultJson}
	if r.Receipt != nil {
		receipt := r.Receipt.API()
		out.Receipt = &receipt
	}
	return out
}

func scope(s api.Scope) *Scope {
	return &Scope{Namespace: s.Namespace, User: s.User, Workspace: s.Workspace}
}
func apiScope(s *Scope) api.Scope {
	if s == nil {
		return api.Scope{}
	}
	return api.Scope{Namespace: s.Namespace, User: s.User, Workspace: s.Workspace}
}
func token(t api.Token) *Token {
	return &Token{DatabaseId: t.DatabaseID, Schema: t.Schema, Revision: t.Revision}
}
func apiToken(t *Token) api.Token {
	if t == nil {
		return api.Token{}
	}
	return api.Token{DatabaseID: t.DatabaseId, Schema: t.Schema, Revision: t.Revision}
}
func SnapshotInput(r api.SnapshotRequest) *SnapshotRequest {
	out := &SnapshotRequest{Scope: scope(r.Scope)}
	if r.At != nil {
		out.At = token(*r.At)
	}
	for _, q := range r.Queries {
		item := &Query{Collection: q.Collection, Keys: q.Keys, Index: q.Index, After: q.After, Limit: int32(q.Limit), IncludeDeleted: q.IncludeDeleted}
		for _, v := range q.Equal {
			item.EqualJson = append(item.EqualJson, []byte(v))
		}
		out.Queries = append(out.Queries, item)
	}
	return out
}
func (r *SnapshotRequest) API() api.SnapshotRequest {
	out := api.SnapshotRequest{Scope: apiScope(r.Scope)}
	if r.At != nil {
		t := apiToken(r.At)
		out.At = &t
	}
	for _, q := range r.Queries {
		item := api.Query{Collection: q.Collection, Keys: q.Keys, Index: q.Index, After: q.After, Limit: int(q.Limit), IncludeDeleted: q.IncludeDeleted}
		for _, v := range q.EqualJson {
			item.Equal = append(item.Equal, json.RawMessage(v))
		}
		out.Queries = append(out.Queries, item)
	}
	return out
}
func SnapshotOutput(r api.SnapshotResponse) *SnapshotResponse {
	out := &SnapshotResponse{Scope: scope(r.Scope), Token: token(r.Token)}
	for _, q := range r.Results {
		item := &QueryResult{Collection: q.Collection, NextAfter: q.NextAfter}
		for _, v := range q.Records {
			item.Records = append(item.Records, record(v))
		}
		out.Results = append(out.Results, item)
	}
	return out
}
func (r *SnapshotResponse) API() api.SnapshotResponse {
	out := api.SnapshotResponse{Scope: apiScope(r.Scope), Token: apiToken(r.Token), Results: []api.QueryResult{}}
	for _, q := range r.Results {
		item := api.QueryResult{Collection: q.Collection, NextAfter: q.NextAfter, Records: []api.Record{}}
		for _, v := range q.Records {
			item.Records = append(item.Records, apiRecord(v))
		}
		out.Results = append(out.Results, item)
	}
	return out
}
func record(r api.Record) *Record {
	return &Record{Collection: r.Collection, Key: r.Key, Version: r.Version, Deleted: r.Deleted, Missing: r.Missing, DataJson: r.Data}
}
func apiRecord(r *Record) api.Record {
	return api.Record{Collection: r.Collection, Key: r.Key, Version: r.Version, Deleted: r.Deleted, Missing: r.Missing, Data: r.DataJson}
}
func BatchInput(r api.BatchRequest) *BatchRequest {
	out := &BatchRequest{Scope: scope(r.Scope), Expected: token(r.Expected), RequestId: r.RequestID}
	for _, m := range r.Mutations {
		out.Mutations = append(out.Mutations, &Mutation{Collection: m.Collection, Key: m.Key, ExpectedVersion: m.ExpectedVersion, Delete: m.Delete, DataJson: m.Data})
	}
	return out
}
func (r *BatchRequest) API() api.BatchRequest {
	out := api.BatchRequest{Scope: apiScope(r.Scope), Expected: apiToken(r.Expected), RequestID: r.RequestId}
	for _, m := range r.Mutations {
		out.Mutations = append(out.Mutations, api.Mutation{Collection: m.Collection, Key: m.Key, ExpectedVersion: m.ExpectedVersion, Delete: m.Delete, Data: m.DataJson})
	}
	return out
}
func ReceiptInput(r api.ReceiptRequest) *ReceiptRequest {
	return &ReceiptRequest{Scope: scope(r.Scope), RequestId: r.RequestID}
}
func (r *ReceiptRequest) API() api.ReceiptRequest {
	return api.ReceiptRequest{Scope: apiScope(r.Scope), RequestID: r.RequestId}
}
func ReceiptOutput(r api.Receipt) *ReceiptResponse {
	out := &ReceiptResponse{RequestId: r.RequestID, Digest: r.Digest, Token: token(r.Token), CommittedAt: r.CommittedAt, ExpiresAt: r.ExpiresAt, Durability: r.Durability}
	for _, v := range r.Versions {
		out.Versions = append(out.Versions, record(v))
	}
	return out
}
func (r *ReceiptResponse) API() api.Receipt {
	out := api.Receipt{RequestID: r.RequestId, Digest: r.Digest, Token: apiToken(r.Token), CommittedAt: r.CommittedAt, ExpiresAt: r.ExpiresAt, Durability: r.Durability}
	for _, v := range r.Versions {
		out.Versions = append(out.Versions, apiRecord(v))
	}
	return out
}
