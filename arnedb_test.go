package arnedb

import (
	"os"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	pDb, err := Open("/tmp/arnedb", "testdb")

	if pDb == nil || err != nil {
		t.Fatal("Open test failed with:", err)
	}

}

func TestCollectionOperations(t *testing.T) {

	_ = os.RemoveAll("/tmp/arnedb/testdb")

	// Veritabanı open yapılır
	pDb, err := Open("/tmp/arnedb", "testdb")
	if pDb == nil || err != nil {
		t.Fatal("Open test failed with:", err)
	}

	// birinci adlı kolleksiyon oluşturulur
	birinci, err := pDb.CreateColl("birinci")
	if err != nil {
		t.Error("Create birinci failed with:", err)
	}

	// ikinci adlı kolleksiyon oluşturulur
	_, err = pDb.CreateColl("ikinci")
	if err != nil {
		t.Error("Create ikinci failed with:", err)
	}

	// üçüncü adlı kolleksiyon oluşturulur
	_, err = pDb.CreateColl("üçüncü")
	if err != nil {
		t.Error("Create üçüncü failed with:", err)
	}

	err = pDb.DeleteColl("ikinci")
	if err != nil {
		t.Error("Delete ikinci failed with:", err)
	}

	// Olmayan bir tane kolleksiyon silmeyi deneriz.
	err = pDb.DeleteColl("hebele")
	if err != nil {
		// bu beklenen bir sonuç
		t.Log("Delete non-existent coll resulted with error as expected. Msg:", err)
	}

	// Kolleksiyona birkaç tane kayıt ekleriz
	e1 := make(map[string]interface{})
	e1["id"] = 34
	e1["name"] = "deneme"
	e1["user"] = "mert"
	e1["unixtime"] = time.Now().Unix()

	_, err = birinci.Add(e1)
	if err != nil {
		t.Fatal("Add(1) Failed with: ", err)
	}

	e1["id"] = 35
	e1["name"] = "hasan"
	e1["unixtime"] = time.Now().Unix()
	_, err = birinci.Add(e1)
	if err != nil {
		t.Fatal("Add(2) Failed with: ", err)
	}

	e1["id"] = 36
	e1["name"] = "Multiline\ndata"
	e1["user"] = "sülüman"
	e1["unixtime"] = time.Now().Unix()
	_, err = birinci.Add(e1)
	if err != nil {
		t.Fatal("Add(3) Failed with: ", err)
	}

	e1["id"] = 40
	e1["name"] = "Mudata"
	e1["user"] = "sülümanos 40"
	e1["unixtime"] = time.Now().Unix()
	_, err = birinci.Add(e1)
	if err != nil {
		t.Fatal("Add(4) Failed with: ", err)
	}

	// Veri sorgulanır:
	// Hatalı sorgulama: Olmayan bir alan istenir
	nper, err := birinci.GetFirst(func(instance RecordInstance) bool {
		return instance["idc"].(float64) > 34
	})
	if err != nil {
		t.Log("Non-existent field error recovery OK. Result:", nper, "  Error:", err)
	}

	// Normal Sorgulama
	dd, err := birinci.GetFirst(func(instance RecordInstance) bool {
		return instance["id"].(float64) > 34
	})

	if err != nil {
		t.Fatal("Error querying:", err)
	}

	if dd == nil {
		t.Error("No data returned")
	}

	t.Logf("GetFirst(id>34) query result: %+v", dd)

	de, err := birinci.GetAll(func(instance RecordInstance) bool {
		return instance["id"].(float64) > 34
	})
	if err != nil {
		t.Fatal("Error querying:", err)
	}

	if de == nil {
		t.Error("No data returned")
	} else {
		if len(de) == 0 {
			t.Error("Empty GetAll result set.")
		} else {
			t.Log("GetAll query results:")
			for _, item := range de {
				t.Logf("\t\t id: %.0f", item["id"])
			}
		}
	}

	// kayıt silme işlemi 36 id'li kayıt silinir.
	err = birinci.DeleteFirst(func(instance RecordInstance) bool {
		return instance["id"].(float64) == 36
	})
	if err != nil {
		t.Fatal("Error querying:", err)
	} else {
		t.Log("DeleteFirst operation returned no error.")
	}

	n, err := birinci.DeleteAll(func(instance RecordInstance) bool {
		return instance["id"].(float64) < 40
	})
	if err != nil {
		t.Fatal("Error querying:", err)
	} else {
		t.Logf("DeleteAll operation removed %d records\n", n)
	}

	// silme işleminden sonra yeniden sorgulama
	dd, err = birinci.GetFirst(func(instance RecordInstance) bool {
		return instance["id"].(float64) > 35
	})

	if err != nil {
		t.Fatal("Error querying:", err)
	}

	if dd == nil {
		t.Error("No data returned")
	}

	t.Logf("GetFirst(id>34) after delete query result (expect id=40): %+v ", dd)

}
