package tables

import (
  "fmt"
  "time"
  "strings"
  "net/http"
  "google.golang.org/appengine"
  "google.golang.org/appengine/log"
  "golang.org/x/oauth2/google"
  bigquery "google.golang.org/api/bigquery/v2"
)

const (
  AlreadyExists = "googleapi: Error 409: Already Exists:"
  scope = bigquery.BigqueryScope
  projectId = "client-side-events"
)

func init() {
  http.HandleFunc("/", handler)
}

func handler(writer http.ResponseWriter, req *http.Request) {
  t := time.Now()
  yyyymmdd := fmt.Sprintf("%4d", t.Year()) + fmt.Sprintf("%02d", t.Month()) + fmt.Sprintf("%02d", t.Day())

  datasetIds := [3]string{
    "Viewer_Events",
    "CAP_Events",
    "OLP_Events",
  }

  tableEntity := bigquery.Table{
    TableReference: &bigquery.TableReference{
      DatasetId: datasetIds[0],
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

  context := appengine.NewContext(req)
  client, err := google.DefaultClient(context, scope)
  if err != nil {
    log.Criticalf(context, "Unable to get default client: %v", err)
    return
  }

  service, err := bigquery.New(client)
  if err != nil {
    log.Criticalf(context, "Unable to create bigquery service: %v", err)
    return
  }

  tableEntity.TableReference.DatasetId = datasetIds[0]

  _, err = service.Tables.Insert(projectId, datasetIds[0], &tableEntity).Do()
  if err != nil && !strings.HasPrefix(err.Error(), AlreadyExists) {
    log.Errorf(context, "Unable to create bigquery service: %v", err)
    return
  }
}
