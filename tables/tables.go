package tables

import (
  "./entities"
  "strings"
  "net/http"
  "google.golang.org/appengine"
  "google.golang.org/appengine/log"
  "golang.org/x/oauth2/google"
  bigquery "google.golang.org/api/bigquery/v2"
)

const (
  AlreadyExists = "googleapi: Error 409: Already Exists:"
  BadAuth = "googleapi: Error 401:"
  scope = bigquery.BigqueryScope
  projectId = "client-side-events"
)

func init() {
  http.HandleFunc("/", createTables)
}

func createTables(writer http.ResponseWriter, req *http.Request) {
  datasetIds := [3]string{
    "Viewer_Events",
    "CAP_Events",
    "OLP_Events",
  }

  context := appengine.NewContext(req)
  client, err := google.DefaultClient(context, scope)
  if err != nil {
    writer.WriteHeader(http.StatusInternalServerError)
    log.Criticalf(context, "Unable to get default client: %v", err)
    return
  }

  service, err := bigquery.New(client)
  if err != nil {
    log.Criticalf(context, "tables/tables.go: Unable to create bigquery service: %v", err)
    writer.WriteHeader(http.StatusUnauthorized)
    return
  }

  for _, datasetId := range datasetIds {
    table, err := entities.Table(datasetId, projectId)

    if err != nil {
      log.Errorf(context, "Error generating table entity: %v", err)
      writer.WriteHeader(http.StatusInternalServerError)
      return
    }

    _, err = service.Tables.Insert(projectId, datasetId, table).Do()

    if err != nil && !strings.HasPrefix(err.Error(), AlreadyExists) {
      log.Errorf(context, "tables/tables.go: Unable to create table: %v", err)
      if (strings.HasPrefix(err.Error(), BadAuth)) {
        writer.WriteHeader(http.StatusUnauthorized)
        return
      }

      writer.WriteHeader(http.StatusInternalServerError)
      return
    }
  }
}
