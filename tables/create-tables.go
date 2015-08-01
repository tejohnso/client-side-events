package tables

import (
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
)

func init() {
  http.HandleFunc("/", handler)
}

func handler(writer http.ResponseWriter, req *http.Request) {
  datasetIds := [3]string{
    "Viewer_Events",
    "CAP_Events",
    "OLP_Events",
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

  for _, datasetId := range datasetIds {
    table := NewTableEntity(datasetId)
    _, err = service.Tables.Insert(projectId, datasetId, table).Do()

    if err != nil && !strings.HasPrefix(err.Error(), AlreadyExists) {
      log.Errorf(context, "Unable to create bigquery service: %v", err)
      return
    }
  }
}
