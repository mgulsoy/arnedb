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

	e2 := make(map[string]interface{})
	e2["id"] = 35
	e2["name"] = "hasan"
	e2["user"] = "mert"
	e2["unixtime"] = time.Now().Unix()
	_, err = birinci.Add(e2)
	if err != nil {
		t.Fatal("Add(2) Failed with: ", err)
	}

	e3 := make(map[string]interface{})
	e3["id"] = 36
	e3["name"] = "Multiline\ndata"
	e3["user"] = "sülüman"
	e3["unixtime"] = time.Now().Unix()
	_, err = birinci.Add(e3)
	if err != nil {
		t.Fatal("Add(3) Failed with: ", err)
	}

	e4 := make(map[string]interface{})
	e4["id"] = 40
	e4["name"] = "Mud,ata"
	e4["user"] = "sülümanos 40"
	e4["unixtime"] = time.Now().Unix()
	_, err = birinci.Add(e4)
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
	dn, err := birinci.DeleteFirst(func(instance RecordInstance) bool {
		return instance["id"].(float64) == 36
	})
	if err != nil {
		t.Fatal("Error querying:", err)
	} else {
		if dn == 1 {
			t.Log("DeleteFirst operation successful.")
		} else {
			t.Error("DeleteFirst operation failed! n=", dn)
		}
	}

	// Tümünü silme işlemi
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

	// Tümünü ekleme işlemi
	dataArray := []RecordInstance{e1, e2, e3, e4}

	_, err = birinci.AddAll(dataArray...)
	if err != nil {
		t.Error("Cannot 'AddAll'", err)
	} else {
		t.Log("AddAll successful.")
	}

	// Update
	ff := func(instance RecordInstance) bool {
		return instance["id"].(float64) == 34
	}

	e1["name"] = "ReplaceSingle yapıldı"
	n, err = birinci.ReplaceSingle(ff, e1)
	if err != nil {
		t.Error("Cannot ReplaceSingle:", err)
	} else {
		if n == 1 {
			t.Log("ReplaceSingle successful.")
		} else {
			t.Error("ReplaceSingle did not return 1 as expected!")
		}
	}

	f2 := func(instance RecordInstance) bool {
		return instance["user"] == "mert"
	}
	e2["user"] = "ReplaceAll Yapıldı"
	n, err = birinci.ReplaceAll(f2, e2)
	if err != nil {
		t.Error("ReplaceAll failed! ", err)
	} else {
		if n == 0 {
			t.Error("ReplaceAll returned 0 records updated. 2 expected")
		} else {
			t.Log("ReplaceAll successful.")
		}
	}

	f36 := QueryPredicate(func(i RecordInstance) bool {
		return i["id"].(float64) == 36
	})
	fUpdt := UpdateFunc(func(ptrRecord *RecordInstance) *RecordInstance {
		(*ptrRecord)["user"] = "Updated Single"
		return ptrRecord
	})

	n, err = birinci.UpdateSingle(f36, fUpdt)
	if err != nil {
		t.Error("UpdateSingle failed")
	} else {
		if n == 0 {
			t.Error("UpdateSingle returned 0 records updated. 1 expected")
		} else {
			t.Log("UpdateSingle successful.")
		}
	}

	f40 := QueryPredicate(func(i RecordInstance) bool {
		return i["id"].(float64) == 40
	})
	fUpdt40 := UpdateFunc(func(ptrRecord *RecordInstance) *RecordInstance {
		(*ptrRecord)["user"] = "Updated All"
		return ptrRecord
	})

	n, err = birinci.UpdateAll(f40, fUpdt40)
	if err != nil {
		t.Error("UpdateAll failed")
	} else {
		if n == 0 {
			t.Error("UpdateAll returned 0 records updated. 2 expected")
		} else {
			t.Log("UpdateAll successful.")
		}
	}
}
