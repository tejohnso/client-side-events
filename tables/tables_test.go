package tables

import (
  "testing"
  "appengine/aetest"
  "net/http/httptest"
)

func TestCreateTables(t *testing.T) {
  inst, err := aetest.NewInstance(nil)
  if err != nil {
    t.Fatalf("Failed to create instance: %v", err)
  }
  defer inst.Close()

  req, err := inst.NewRequest("GET", "/", nil)
  if err != nil {
    t.Fatalf("Failed to create req: %v", err)
  }

  w := httptest.NewRecorder()

  createTables(w, req)

  if w.Code != 200 {
    t.Error("Create table did not succeed")
  }
}
