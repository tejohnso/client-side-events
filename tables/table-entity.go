package tables

import (
  "time"
  "fmt"
  bigquery "google.golang.org/api/bigquery/v2"
)
const (
  projectId = "client-side-events"
)

func NewTableEntity(datasetId string) (tableEntity *bigquery.Table) {
  t := time.Now()
  yyyymmdd := fmt.Sprintf("%4d", t.Year()) + fmt.Sprintf("%02d", t.Month()) + fmt.Sprintf("%02d", t.Day())

  tableEntity = &bigquery.Table{
    TableReference: &bigquery.TableReference{
      DatasetId: datasetId,
      ProjectId: projectId,
      TableId: "events" + yyyymmdd,
    },
    Schema: &bigquery.TableSchema{
      Fields: []*bigquery.TableFieldSchema{
        &bigquery.TableFieldSchema{
          Mode: "REQUIRED",
          Name: "event",
          Type: "STRING",
        },
        &bigquery.TableFieldSchema{
          Mode: "NULLABLE",
          Name: "display_id",
          Type: "STRING",
        },
        &bigquery.TableFieldSchema{
          Mode: "NULLABLE",
          Name: "viewer_version",
          Type: "STRING",
        },
        &bigquery.TableFieldSchema{
          Mode: "REQUIRED",
          Name: "ts",
          Type: "TIMESTAMP",
        },
      },
    },
  }

  return
}
