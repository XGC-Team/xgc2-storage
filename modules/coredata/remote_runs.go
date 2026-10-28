package coredata

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sort"

	"github.com/XGC-Team/xgc2-storage/engine"
	"github.com/XGC-Team/xgc2-storage/modules/coredata/model"
)

// A page inspects at most 16 pending owner blobs (at most 8 MiB), independently
// of matching link count. History and full execution payloads are never read.
const remoteOwnerScanLimit = 16

func validateRemoteLink(link model.RemoteRunLink) error {
	for _, key := range []string{link.OwnerRunID, link.RootRunID, link.TargetID, link.RunID, link.CallNodeID} {
		if !textKey(key) {
			return failure("invalid_argument", "remote control identities must be bounded text keys")
		}
	}
	return optionalKey("remote boot", link.BootID)
}

func remoteLinks(raw []byte, count int) ([]model.RemoteRunLink, error) {
	if len(raw) == 0 && count == 0 {
		return nil, nil
	}
	if len(raw) > model.MaxRemoteRunLinksBytes || count < 0 || count > model.MaxRemoteRunLinks {
		return nil, failure("data_loss", "invalid remote ownership capacity")
	}
	var links []model.RemoteRunLink
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&links) != nil || len(links) != count || decoder.Decode(new(any)) != io.EOF {
		return nil, failure("data_loss", "invalid remote ownership payload")
	}
	seen := map[string]bool{}
	for _, link := range links {
		key := link.TargetID + "\x00" + link.RunID
		if validateRemoteLink(link) != nil || seen[key] {
			return nil, failure("data_loss", "invalid or repeated remote ownership identity")
		}
		seen[key] = true
	}
	return links, nil
}

// ReserveRemoteRun precedes a remote Start. A later reservation may bind an
// initially unknown boot exactly once; a retry never removes that binding.
func (s *Store) ReserveRemoteRun(ctx context.Context, link model.RemoteRunLink) error {
	return s.changeRemoteRun(ctx, link, false)
}

// ResolveRemoteRun requires an exact terminal/joined or definitive unsent
// identity. A Stop acceptance alone is not a reason to call it.
func (s *Store) ResolveRemoteRun(ctx context.Context, link model.RemoteRunLink) error {
	return s.changeRemoteRun(ctx, link, true)
}

func (s *Store) changeRemoteRun(ctx context.Context, link model.RemoteRunLink, resolve bool) error {
	if err := validateRemoteLink(link); err != nil {
		return err
	}
	_, err := write(ctx, s, engine.Durable, func(ctx context.Context, tx *sql.Tx, scope string) (bool, error) {
		var root, target, status string
		var raw []byte
		var count int
		err := tx.QueryRowContext(ctx, "SELECT root_run_id,target_id,status,pending_remote_json,pending_remote_count FROM runs WHERE scope=? AND id=?", scope, link.OwnerRunID).Scan(&root, &target, &status, &raw, &count)
		if errors.Is(err, sql.ErrNoRows) {
			return false, failure("not_found", "remote control owner is unavailable")
		}
		if err != nil {
			return false, err
		}
		if root != link.RootRunID || target == link.TargetID {
			return false, failure("failed_precondition", "remote control owner lineage or target disagrees")
		}
		links, err := remoteLinks(raw, count)
		if err != nil {
			return false, err
		}
		found, changed := false, false
		for i, current := range links {
			if current.TargetID != link.TargetID || current.RunID != link.RunID {
				continue
			}
			found = true
			if current.OwnerRunID != link.OwnerRunID || current.RootRunID != link.RootRunID || current.CallNodeID != link.CallNodeID ||
				(resolve && current.BootID != link.BootID) || (!resolve && current.BootID != "" && link.BootID != "" && current.BootID != link.BootID) {
				return false, failure("failed_precondition", "remote control identity or boot conflicts")
			}
			if resolve {
				links = append(links[:i], links[i+1:]...)
				changed = true
			} else if current.BootID == "" && link.BootID != "" {
				links[i].BootID = link.BootID
				changed = true
			}
			break
		}
		if !found && !resolve {
			if !model.RunStatus(status).Open() {
				return false, failure("failed_precondition", "closed owner cannot reserve a new remote Run")
			}
			links = append(links, link)
			changed = true
		}
		if !changed {
			return false, nil
		}
		sort.Slice(links, func(i, j int) bool {
			if links[i].TargetID != links[j].TargetID {
				return links[i].TargetID < links[j].TargetID
			}
			return links[i].RunID < links[j].RunID
		})
		var next []byte
		if len(links) > 0 {
			next, err = json.Marshal(links)
		}
		if err != nil {
			return false, err
		}
		if len(links) > model.MaxRemoteRunLinks || len(next) > model.MaxRemoteRunLinksBytes {
			return false, failure("resource_exhausted", "remote control owner capacity reached")
		}
		delta := int64(len(next) - len(raw))
		if delta > 0 {
			err = reserve(ctx, tx, scope, 0, delta)
		} else if delta < 0 {
			err = release(ctx, tx, scope, 0, -delta)
		}
		if err != nil {
			return false, err
		}
		_, err = tx.ExecContext(ctx, "UPDATE runs SET pending_remote_json=?,pending_remote_count=?,bytes=bytes+? WHERE scope=? AND id=?", nonNilBytes(next), len(links), delta, scope, link.OwnerRunID)
		return true, err
	})
	return err
}

func nonNilBytes(raw []byte) []byte {
	if raw == nil {
		return []byte{}
	}
	return raw
}

type remoteRunCursor struct {
	Target   string `json:"target"`
	Owner    string `json:"owner"`
	Run      string `json:"run,omitempty"`
	Complete bool   `json:"complete,omitempty"`
}

func remotePageCursor(position remoteRunCursor) string {
	raw, _ := json.Marshal(position)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func (s *Store) ListPendingRemoteRuns(ctx context.Context, target, cursor string, limit int) (model.RemoteRunPage, error) {
	if !textKey(target) || limit < 1 || limit > model.MaxRemoteRunPageSize {
		return model.RemoteRunPage{}, failure("invalid_argument", "invalid pending remote page")
	}
	position := remoteRunCursor{Target: target}
	if cursor != "" {
		if len(cursor) > 16384 {
			return model.RemoteRunPage{}, failure("invalid_argument", "invalid pending remote cursor")
		}
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(raw) > 8192 {
			return model.RemoteRunPage{}, failure("invalid_argument", "invalid pending remote cursor")
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&position) != nil || decoder.Decode(new(any)) != io.EOF || position.Target != target || !textKey(position.Owner) || position.Complete && position.Run != "" || !position.Complete && !textKey(position.Run) {
			return model.RemoteRunPage{}, failure("invalid_argument", "pending remote cursor identity disagrees")
		}
	}
	return read(ctx, s, func(ctx context.Context, tx *sql.Tx, scope string) (model.RemoteRunPage, error) {
		comparison := ">="
		if position.Complete {
			comparison = ">"
		}
		rows, err := tx.QueryContext(ctx, "SELECT id,root_run_id,pending_remote_json,pending_remote_count FROM runs INDEXED BY runs_pending_remote WHERE scope=? AND pending_remote_count>0 AND id"+comparison+"? ORDER BY id LIMIT ?", scope, position.Owner, remoteOwnerScanLimit+1)
		if err != nil {
			return model.RemoteRunPage{}, err
		}
		defer rows.Close()
		var matches []model.RemoteRunLink
		lastOwner, moreOwners, scanned := "", false, 0
		for rows.Next() {
			if scanned == remoteOwnerScanLimit {
				moreOwners = true
				break
			}
			var owner, root string
			var raw []byte
			var count int
			if err := rows.Scan(&owner, &root, &raw, &count); err != nil {
				return model.RemoteRunPage{}, err
			}
			links, err := remoteLinks(raw, count)
			if err != nil {
				return model.RemoteRunPage{}, err
			}
			for _, link := range links {
				if link.OwnerRunID != owner || link.RootRunID != root {
					return model.RemoteRunPage{}, failure("data_loss", "remote control row lineage disagrees")
				}
				if link.TargetID == target && (owner != position.Owner || link.RunID > position.Run) {
					matches = append(matches, link)
				}
			}
			lastOwner = owner
			scanned++
		}
		if err := rows.Err(); err != nil {
			return model.RemoteRunPage{}, err
		}
		page := model.RemoteRunPage{Links: matches}
		if len(matches) > limit {
			page.Links = matches[:limit]
			last := page.Links[len(page.Links)-1]
			page.Next = remotePageCursor(remoteRunCursor{Target: target, Owner: last.OwnerRunID, Run: last.RunID})
		} else if moreOwners {
			page.Next = remotePageCursor(remoteRunCursor{Target: target, Owner: lastOwner, Complete: true})
		}
		return page, nil
	})
}
