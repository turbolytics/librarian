package catalog

import "time"

/*
The catalog is a record of what has been processed.
The catalog is a primitive for verifying, inventorying and auditing
data operations.
*/

// Catalog represents the catalog of records that have been processed
type Catalog struct {
	StartTime           time.Time `json:"start_time"`
	EndTime             time.Time `json:"end_time"`
	Source              string    `json:"source"`
	NumSourceRecords    int       `json:"num_source_records"`
	NumRecordsProcessed int       `json:"num_records_processed"`
	Completed           bool      `json:"completed"`
}
