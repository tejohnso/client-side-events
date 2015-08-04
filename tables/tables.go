package tables

import (
  "time"
  "fmt"
  "strings"
  "net/http"
  "google.golang.org/appengine"
  "golang.org/x/net/context"
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

  t := time.Now()
  for i := 1; i < 3; i++ {
    t = t.AddDate(0, 0, 1)
    log.Infof(context, "tables/tables.go: Creating tables for %v-%v-%v", t.Year(), int(t.Month()), t.Day())
    insertTablesForTime(t, service, errorHandler(writer, context))
  }
}

func insertTablesForTime(t time.Time, service *bigquery.Service, errorHandler func(error)) {
  yyyymmdd := fmt.Sprintf("%4d", t.Year()) + fmt.Sprintf("%02d", t.Month()) + fmt.Sprintf("%02d", t.Day())
  tableId := "events" + yyyymmdd

  datasetIds := [3]string{
    "Viewer_Events",
    "CAP_Events",
    "OLP_Events",
  }

  for _, datasetId := range datasetIds {
    table, err := NewTableEntity(datasetId, projectId, tableId)
    errorHandler(err)

    _, err = service.Tables.Insert(projectId, datasetId, table).Do()
    errorHandler(err)
  }
}

func errorHandler(writer http.ResponseWriter, context context.Context) (func(error)) {
  return func (err error) {
    if err == nil {return}

    if strings.HasPrefix(err.Error(), AlreadyExists) {
      log.Infof(context, "tables.tables.go: table exists")
      return
    }

    log.Errorf(context, "tables/tables.go: Could not create table: ", err)

    if strings.HasPrefix(err.Error(), BadAuth) {
      writer.WriteHeader(http.StatusUnauthorized)
      return
    }

    writer.WriteHeader(http.StatusInternalServerError)
    return
  }
}
