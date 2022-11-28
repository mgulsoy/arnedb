package arnedb

import (
	"fmt"
	"os"
	"testing"
	"time"
)

type SampleNestedType struct {
	IntegerValue int
	StringValue  string
	FloatValue   float64
}

type SampleRecordType struct {
	Id         int
	Name       string
	Nested     SampleNestedType
	ArrayValue []string
}

func TestOpen(t *testing.T) {
	pDb, err := Open("testdb", "testdb")

	if pDb == nil || err != nil {
		t.Fatal("Open test failed with:", err)
	}

}

func TestCollectionOperations(t *testing.T) {

	_ = os.RemoveAll("testdb/testdb")

	// Veritabanı open yapılır
	pDb, err := Open("testdb", "testdb")
	if pDb == nil || err != nil {
		t.Fatal("Open test failed with:", err)
	}

	// Request a collection that is not present
	nec := pDb.GetColl("non-existent")
	if nec == nil {
		t.Log("Request non-existent collection success")
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
	ucuncu, err := pDb.CreateColl("üçüncü")
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

	err = birinci.Add(e1)
	if err != nil {
		t.Fatal("Add(1) Failed with: ", err)
	}

	e2 := make(map[string]interface{})
	e2["id"] = 35
	e2["name"] = "hasan"
	e2["user"] = "mert"
	e2["unixtime"] = time.Now().Unix()
	err = birinci.Add(e2)
	if err != nil {
		t.Fatal("Add(2) Failed with: ", err)
	}

	e3 := make(map[string]interface{})
	e3["id"] = 36
	e3["name"] = "Multiline\ndata"
	e3["user"] = "sülüman"
	e3["unixtime"] = time.Now().Unix()
	err = birinci.Add(e3)
	if err != nil {
		t.Fatal("Add(3) Failed with: ", err)
	}

	e4 := make(map[string]interface{})
	e4["id"] = 40
	e4["name"] = "Mud,ata"
	e4["user"] = "sülümanos 40"
	e4["unixtime"] = time.Now().Unix()
	err = birinci.Add(e4)
	if err != nil {
		t.Fatal("Add(4) Failed with: ", err)
	}

	// Get the count of 'birinci' collection
	qCount := func(q RecordInstance) bool {
		return true
	}

	bcount, err := birinci.Count(qCount)
	if err != nil {
		t.Error("Failed to get count: ", err.Error())
	} else {
		t.Log(fmt.Sprintf("Expected count: %d", bcount))
	}

	t1 := SampleRecordType{
		Id:         12,
		Name:       "First Sample Type Instance",
		ArrayValue: []string{"one", "two", "three"},
		Nested: SampleNestedType{
			IntegerValue: 32,
			StringValue:  "Nested string",
			FloatValue:   32.45,
		},
	}

	t2 := SampleRecordType{
		Id:         13,
		Name:       "Second Sample Type Instance",
		ArrayValue: []string{"four", "five", "six"},
		Nested: SampleNestedType{
			IntegerValue: 52,
			StringValue:  "Another nested string",
			FloatValue:   12.3456,
		},
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

	nAddAll, err := birinci.AddAll(dataArray...)
	if err != nil {
		t.Error("Cannot 'AddAll'", err)
	} else {
		t.Logf("AddAll successful. %d records added.", nAddAll)
	}

	// Update
	ff := func(instance RecordInstance) bool {
		return instance["id"].(float64) == 34
	}

	e1["name"] = "ReplaceFirst yapıldı"
	n, err = birinci.ReplaceFirst(ff, e1)
	if err != nil {
		t.Error("Cannot ReplaceFirst:", err)
	} else {
		if n == 1 {
			t.Log("ReplaceFirst successful.")
		} else {
			t.Error("ReplaceFirst did not return 1 as expected!")
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

	n, err = birinci.UpdateFirst(f36, fUpdt)
	if err != nil {
		t.Error("UpdateFirst failed")
	} else {
		if n == 0 {
			t.Error("UpdateFirst returned 0 records updated. 1 expected")
		} else {
			t.Log("UpdateFirst successful.")
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

	// typed structure tests
	err = ucuncu.Add(t1)
	if err != nil {
		t.Fatal("Failed adding typed structure t1:", err.Error())
	}

	err = ucuncu.Add(t2)
	if err != nil {
		t.Fatal("Failed adding typed structure t2:", err.Error())
	}

	t.Log("t1 and t1 Add success")

	recordAs, err := GetFirstAs[SampleRecordType](ucuncu, func(i *SampleRecordType) bool {
		return i.Id == 13
	})
	if err != nil {
		t.Fatal("Error on generics request:", err.Error())
	} else {
		if recordAs != nil {
			t.Log("GetFirstAs: ", recordAs.Id, recordAs.Name, " Success")
		} else {
			t.Log("GetFirstAs: No records found. Success")
		}

	}

	startMark := time.Now()
	recordsAs, err := GetAllAs[SampleRecordType](ucuncu, func(i *SampleRecordType) bool {
		return true // we want all
	})
	if err != nil {
		t.Fatal("Error on generics get all: ", err.Error())
	} else {
		diff := time.Now().Sub(startMark)
		t.Log("Success GetAllAs. Len: ", len(recordsAs), diff)
	}

	//Query typed structure
	var rt1 SampleRecordType
	var rtPredicate = func(instance interface{}) bool {
		//fmt.Println("type of instance: ", reflect.TypeOf(instance))
		i := instance.(*SampleRecordType) // this typecast is required
		return i.Id == 13
	}
	found, err := ucuncu.GetFirstAsInterface(rtPredicate, &rt1)
	if err != nil {
		t.Fatal("Error on interface request:", err.Error())
	}

	if found {
		t.Logf("Record found: %+v", rt1)
	} else {
		t.Log("Record not found.")
	}

	startMark = time.Now()
	var rt2Predicate = func(instance interface{}) bool {
		//fmt.Println("type of instance: ", reflect.TypeOf(instance))
		//i := instance.(*SampleRecordType) // this typecast is required
		return true
	}

	var resultCollection = make([]SampleRecordType, 0)
	var rt2FoundCallback = func(instance interface{}) bool {
		i := *instance.(*SampleRecordType)
		resultCollection = append(resultCollection, i)
		return true
	}

	n, err = ucuncu.GetAllAsInterface(rt2Predicate, rt2FoundCallback, &rt1)
	if err != nil {
		t.Fatal("Error on interface request:", err.Error())
	}

	if n > 0 {
		diff := time.Now().Sub(startMark)
		t.Logf("%d Records found: %T, %s", n, resultCollection, diff)
		for _, k := range resultCollection {
			t.Logf("\t ->%T : %+v", k, k)
		}
	} else {
		t.Log("Record not found.")
	}

}

func BenchmarkMemAndGenerics(b *testing.B) {
	b.ReportAllocs()
	pDb, err := Open("testdb", "testdb")
	if pDb == nil || err != nil {
		b.Fatal("Open test failed with:", err)
	}

	ucuncu := pDb.GetColl("üçüncü")
	if ucuncu == nil {
		b.Error("Create üçüncü failed with:", err)
	}

	recordAs, err := GetFirstAs[SampleRecordType](ucuncu, func(i *SampleRecordType) bool {
		return i.Id == 36
	})
	if err != nil {
		b.Fatal("Error on interface request:", err.Error())
	} else {
		b.Log("GetFirstAs: ", recordAs.Id, recordAs.Name, " Success")
	}

}
