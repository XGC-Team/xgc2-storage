package client

import "github.com/XGC-Team/xgc2-storage/api"

// A projection is an explicit wire contract. A peer must never silently return
// full Data when it did not understand a header query (or the reverse).
func validateSnapshotRepresentation(request api.SnapshotRequest, response api.SnapshotResponse) error {
	if len(request.Queries) != len(response.Results) {
		return &api.Error{Code: "data_loss", Message: "snapshot result count differs from its query plan"}
	}
	for i, query := range request.Queries {
		page := response.Results[i]
		if page.Collection != query.Collection {
			return &api.Error{Code: "data_loss", Message: "snapshot result collection differs from its query plan"}
		}
		if query.HeaderIndex != "" && len(page.Records) != len(query.Keys) {
			return &api.Error{Code: "data_loss", Message: "snapshot header result omits an exact key"}
		}
		for j, record := range page.Records {
			if query.HeaderIndex == "" {
				if record.Header != nil {
					return &api.Error{Code: "data_loss", Message: "snapshot returned a projection instead of full Data"}
				}
			} else if record.Header == nil || len(record.Data) != 0 || record.Header.Index != query.HeaderIndex || record.Header.Bytes < 0 || record.Key != query.Keys[j] {
				return &api.Error{Code: "data_loss", Message: "snapshot header representation differs from its exact query plan"}
			}
		}
	}
	return nil
}
