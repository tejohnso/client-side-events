package tables

import (
  "fmt"
  "strings"
  bigquery "google.golang.org/api/bigquery/v2"
)

var schemas = make(map[string]*bigquery.TableSchema)

func init() {
  schemas["Viewer"] = &bigquery.TableSchema{
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
        Name: "player_version",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "viewer_version",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "player_name",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "os",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "REQUIRED",
        Name: "ts",
        Type: "TIMESTAMP",
      },
    },
  }
  schemas["OLP"] = &bigquery.TableSchema{
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
        Name: "chrome_version",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "os",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "ip",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "olp_version",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "REQUIRED",
        Name: "ts",
        Type: "TIMESTAMP",
      },
    },
  }
  schemas["CAP"] = &bigquery.TableSchema{
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
        Name: "chrome_version",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "os",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "ip",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "cap_version",
        Type: "STRING",
      },
      &bigquery.TableFieldSchema{
        Mode: "NULLABLE",
        Name: "time_millis",
        Type: "INTEGER",
      },
      &bigquery.TableFieldSchema{
        Mode: "REQUIRED",
        Name: "ts",
        Type: "TIMESTAMP",
      },
    },
  }
}

func NewTableEntity(datasetId string, projectId string, tableId string) (tableEntity *bigquery.Table, err error) {
  schema := schemas[strings.Split(datasetId, "_")[0]]
  if schema == nil {
    err = fmt.Errorf("entities/table.go: no schema for %v", datasetId)
    return
  }

  tableEntity = &bigquery.Table{
    TableReference: &bigquery.TableReference{
      DatasetId: datasetId,
      ProjectId: projectId,
      TableId: tableId,
    },
    Schema: schema,
  }

  return
}
