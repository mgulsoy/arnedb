package arnedb

import "testing"

func TestOpen(t *testing.T) {
	pDb, err := Open("/tmp/arnedb", "testdb")

	if pDb == nil || err != nil {
		t.Fatal("Open test failed with:", err)
	}
}
