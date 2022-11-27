package arnedb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const firstChunkName = "00.json"
const maxChunkSize = 1024 * 1024 // 1MB
const recordSepChar = 10         // --> \n
const recordSepStr = "\n"

// RecordInstance represents a record instance read from data file. It is actually a map.
type RecordInstance map[string]interface{}

// QueryPredicate is a function type receiving row instance and returning bool
type QueryPredicate func(instance RecordInstance) bool

// QueryPredicateAsInterface is a function type receiving record instance as given type and returning bool
type QueryPredicateAsInterface func(instance interface{}) bool

// UpdateFunc alters the data matched by predicate
type UpdateFunc func(ptrRecord *RecordInstance) *RecordInstance

// Add function appends data into a collection
func (coll *Coll) Add(data interface{}) error {

	// Kolleksiyonlar chunkXX.json adı verilen yığınlara ayrılır. Her bir yığın max 1 MB büyüklüğe kadar
	// büyüyebilir.

	payload, err := json.Marshal(data)
	if err != nil {
		// veriyi paketlemekte sorun
		return errors.New(fmt.Sprintf("cannot marshal data: %s", err.Error()))
	}

	// Coll var mı ona bakılır. Yoksa hata...
	_, err = os.Stat(coll.dbpath)
	if os.IsNotExist(err) {
		return errors.New(fmt.Sprintf("collection does not exist: %s", err.Error()))
	}

	// Coll var. En son chunk bulunur.
	lastChunk, err := coll.createChunk()
	if err != nil {
		return err
	}

	// Elimizde en son chunk var.
	chunkPath := filepath.Join(coll.dbpath, (*lastChunk).Name())
	f, err := os.OpenFile(chunkPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New(fmt.Sprintf("cannot open chunk to add data: %s", err.Error()))
	}

	// Kayıt sonu karakteri eklenir
	payload = append(payload, byte(recordSepChar))
	write, err := f.Write(payload)
	if err != nil {
		return errors.New(fmt.Sprintf("cannot append chunk: %s", err.Error()))
	}

	if write != len(payload) {
		return errors.New(fmt.Sprintf("append to chunk failed with: %d bytes diff", len(payload)-write))
	}
	err = f.Close()
	if err != nil {
		return errors.New(fmt.Sprintf("append failed to clos file: %s", err.Error()))
	}

	// işlem başarılı
	return nil
}

// AddAll function appends multiple data into a collection. If one fails, no data will be committed to storage. Thus,
// this function acts like a transaction. This function is a variadic function which accepts a SLICE as an argument:
//
//	d := []RecordInstance{ a, b, c}
//	AddAll(d...)
//
// Or can be called like:
//
//	AddAll(d1,d2,d3)
func (coll *Coll) AddAll(data ...RecordInstance) (int, error) {

	n := 0
	_, err := os.Stat(coll.dbpath)
	if os.IsNotExist(err) {
		return n, errors.New(fmt.Sprintf("collection does not exist: %s", err.Error()))
	}

	// Coll var. En son chunk bulunur.
	lastChunk, err := coll.createChunk()
	if err != nil {
		return n, err
	}

	bufferStore := make([]byte, 512*len(data)) // her eleman için 512 byte ayır
	buffer := bytes.NewBuffer(bufferStore)
	buffer.Reset()

	// Ekleme işlemini hafızada gerçekleştir.
	// TODO: Test payload allocation performance
	for _, dataElement := range data {
		payload, err := json.Marshal(dataElement)
		if err != nil {
			// veriyi paketlemekte sorun
			return 0, errors.New(fmt.Sprintf("cannot marshal data: %s", err.Error()))
		}

		// Tampon belleğe kaydı ekle
		buffer.Write(payload)
		// Kayıt sonu karakterini ekle
		buffer.WriteString(recordSepStr)
		n++
	}

	// Buraya kadar kod kırılmamışsa diske yazabiliriz.
	// Elimizde en son chunk var.
	chunkPath := filepath.Join(coll.dbpath, (*lastChunk).Name())
	f, err := os.OpenFile(chunkPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("cannot open chunk to add data: %s", err.Error()))
	}

	// Şimdilik yazılan byte sayısı ile ilgilenmiyoruz
	_, err = buffer.WriteTo(f)
	if err != nil {
		_ = f.Close()
		return 0, errors.New(fmt.Sprintf("cannot append chunk: %s", err.Error()))
	}

	err = f.Close()
	if err != nil {
		return 0, errors.New(fmt.Sprintf("append failed to close file: %s", err.Error()))
	}

	// işlem başarılı
	return n, nil
}

// GetFirst function queries and gets the first match of the query.
// The function returns nil if no data found.
func (coll *Coll) GetFirst(predicate QueryPredicate) (result RecordInstance, err error) {
	chunks, err := coll.getChunks()
	if err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return nil, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			result = nil
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	var data RecordInstance
	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return nil, err
		}

		scn := bufio.NewScanner(f)
		dataMatched := false
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				continue
			}
			_ = json.Unmarshal(line, &data) // TODO: Handle error
			dataMatched = predicate(data)
			if dataMatched {
				break
			}
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
		if dataMatched {
			return data, nil
		}
	}

	return nil, nil
}

// GetFirstAs function queries given coll and gets the first match of the query. This function uses generics.
// Returns nil if no data found.
func GetFirstAs[T any](coll *Coll, predicate func(i *T) bool) (result *T, err error) {
	chunks, err := coll.getChunks()
	if err != nil {
		return nil, err // marks not found
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return nil, nil // no data
	}

	var f *os.File
	defer func() { // predicate içindeki hatayı yakala
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return nil, err
		}

		dec := json.NewDecoder(f)
		var m T
		predicateResult := false
		for {
			err = dec.Decode(&m)
			if err == io.EOF {
				// eof
				break
			} else if err != nil {
				//return false, err
				continue // skip this record
			}
			predicateResult = predicate(&m)
			if predicateResult == true {
				break
			}
		}

		_ = f.Close() // TODO: Handle error
		f = nil       // temizle

		if predicateResult == true {
			return &m, nil
		}
	}

	return nil, nil

}

// GetAllAs function queries given coll and returns all for the predicate match. This function uses generics.
// Returns a slice of data pointers. If nothing is found then empty slice is returned
func GetAllAs[T any](coll *Coll, predicate func(i *T) bool) (result []*T, err error) {
	chunks, err := coll.getChunks()
	if err != nil {
		return nil, err // marks not found
	}

	result = make([]*T, 0) // sonuç için bir kolleksiyon ayarla

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return result, nil // no data
	}

	var f *os.File
	defer func() { // predicate içindeki hatayı yakala
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return nil, err
		}

		dec := json.NewDecoder(f)
		predicateResult := false
		for {
			var m T
			err = dec.Decode(&m)
			if err == io.EOF {
				// eof
				break
			} else if err != nil {
				//return false, err
				continue // skip this record
			}
			predicateResult = predicate(&m)
			if predicateResult == true {
				result = append(result, &m)
			}
		}

		_ = f.Close() // TODO: Handle error
		f = nil       // temizle

		return result, nil
	}

	return result, nil
}

// GetFirstAsInterface function queries and gets the first match of the query. The query result can be found in the
// holder argument. The function returns a boolean value indicating data is found or not.
func (coll *Coll) GetFirstAsInterface(predicate QueryPredicateAsInterface, holder interface{}) (found bool, err error) {
	chunks, err := coll.getChunks()
	if err != nil {
		return false, err // marks not found
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return false, nil // no data
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			found = false
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return false, err
		}

		scn := bufio.NewScanner(f)
		dataMatched := false
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				continue
			}
			err = json.Unmarshal(line, holder)
			if err != nil {
				// error on unmarshal operation
				continue // skip this record
			}
			el := holder
			dataMatched = predicate(el) // evaluate predicate
			if dataMatched {
				found = true
				break
			}
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
		if dataMatched {
			return true, nil
		}
	}

	holder = nil // reset holder.

	return false, nil
}

// GetAll function queries and gets all the matches of the query predicate.
func (coll *Coll) GetAll(predicate QueryPredicate) (result []RecordInstance, err error) {

	chunks, err := coll.getChunks()
	if err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return nil, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			result = nil
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	result = make([]RecordInstance, 0)
	dataMatched := false

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return nil, err
		}

		scn := bufio.NewScanner(f)
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				continue
			}
			var data RecordInstance
			_ = json.Unmarshal(line, &data) // TODO: Handle error
			dataMatched = predicate(data)
			if dataMatched {
				result = append(result, data)
			}
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
	}

	return result, nil
}

// Count function returns the count of matched records with the predicate function
func (coll *Coll) Count(predicate QueryPredicate) (n int, err error) {
	n = 0
	chunks, err := coll.getChunks()
	if err != nil {
		return n, err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return n, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			n = 0
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	dataMatched := false
	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return 0, err
		}

		scn := bufio.NewScanner(f)
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				continue
			}
			var data RecordInstance
			_ = json.Unmarshal(line, &data) // TODO: Handle error
			dataMatched = predicate(data)
			if dataMatched {
				n++
			}
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // cleanup
	}

	return n, nil
}

// GetAllAsInterface function queries and gets all the matches of the query predicate. Returns the
// number of record found or 0 if not. Data is sent into harvestCallback function. So you can harvest
// the data. There is no generics in GO. So user must handle the type conversion.
func (coll *Coll) GetAllAsInterface(predicate QueryPredicateAsInterface, harvestCallback QueryPredicateAsInterface, holder interface{}) (n int, err error) {

	n = 0 // init
	chunks, err := coll.getChunks()
	if err != nil {
		return n, err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return n, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			//fmt.Errorf("recover??? %+v", r)
			n = 0
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	dataMatched := false
	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return 0, err
		}

		scn := bufio.NewScanner(f)
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				continue
			}

			err = json.Unmarshal(line, holder) // TODO: Handle error
			if err != nil {
				// if an error occurs skip it
				continue
			}
			dataMatched = predicate(holder)
			if dataMatched {
				harvestCallback(holder)
				n++
			}
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
	}

	return n, nil
}

// DeleteFirst function deletes the first match of the predicate and returns the count of deleted
// records. n = 1 if a deletion occurred, n = 0 if none.
func (coll *Coll) DeleteFirst(predicate QueryPredicate) (n int, err error) {
	chunks, err := coll.getChunks()
	n = 0
	if err != nil {
		return n, err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return n, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()
	var data RecordInstance
	var bufferStore = make([]byte, 2*1024*1024) // 2 mb buffer
	buffer := bytes.NewBuffer(bufferStore)

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return n, err
		}

		scn := bufio.NewScanner(f)
		buffer.Reset()
		dataMatched := false
		anyMatchesOccured := false

		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				dataMatched = false
			} else {
				_ = json.Unmarshal(line, &data) // TODO: Handle error
				dataMatched = predicate(data)
			}
			if !dataMatched {
				// predicate sonucu olumsuz. Bu durumda orjinal data yerine yazılır.
				buffer.Write(line)
				buffer.WriteString(recordSepStr)
			} else {
				// Sonuç bulunmuş. Bu sonuç yerine \n yazılır. Satır numarası değişmez! Fakat bu işlem sadece
				// ilk sonuç için yapılır.
				// Satır numarası daha sonradan indexleme için kullanılacak!
				if !anyMatchesOccured {
					buffer.WriteString(recordSepStr)
				}
			}
			anyMatchesOccured = anyMatchesOccured || dataMatched
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
		if anyMatchesOccured {
			//dosyada düzeltme yapılmış demektir. Bu durumda buffer, işlem yapılan chunk üzerine yazılır.
			f, err = os.Create(chunkPath) // Truncate file
			if err != nil {
				return n, err
			}
			//_, err := f.Write(bufferStore)
			_, err = buffer.WriteTo(f)
			if err != nil {
				// yazma hatası
				return n, err
			}
			_ = f.Close()
			f = nil
			n++
			break // Chunk loop kır.
		}
	} //end chunks

	return n, nil
}

// DeleteAll function deletes all the matches of the predicate and returns the count of deletions.
// n = 0 if no deletions occurred.
func (coll *Coll) DeleteAll(predicate QueryPredicate) (n int, err error) {
	chunks, err := coll.getChunks()
	n = 0
	if err != nil {
		return n, err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return n, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			n = 0
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()
	var data RecordInstance
	var bufferStore = make([]byte, 2*1024*1024) // 2 mb buffer
	buffer := bytes.NewBuffer(bufferStore)

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return 0, err
		}

		scn := bufio.NewScanner(f)
		buffer.Reset()
		dataMatched := false
		anyMatchesOccured := false
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				dataMatched = false
			} else {
				// Satır boş değil
				_ = json.Unmarshal(line, &data) // TODO: Handle error
				dataMatched = predicate(data)
			}

			if !dataMatched {
				// predicate sonucu olumsuz. Bu durumda orjinal data yerine yazılır.
				buffer.Write(line)
			} else {
				n++
			}
			buffer.WriteString(recordSepStr)

			anyMatchesOccured = anyMatchesOccured || dataMatched
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
		if anyMatchesOccured {
			//dosyada düzeltme yapılmış demektir. Bu durumda buffer, işlem yapılan chunk üzerine yazılır.
			f, err = os.Create(chunkPath) // Truncate file
			if err != nil {
				return 0, err
			}
			//_, err := f.Write(bufferStore)
			_, err = buffer.WriteTo(f)
			if err != nil {
				// yazma hatası
				return 0, err
			}
			_ = f.Close()
			f = nil
		}
	} //end chunks

	return n, nil
}

// ReplaceFirst replaces the first match of the predicate with the newData and returns
// the count of updates. Obviously the return value is 1 if update is successful and 0 if not.
// Update is the most costly operation. The library does not provide a method to update parts of a
// document since document is not known to the system.
func (coll *Coll) ReplaceFirst(predicate QueryPredicate, newData interface{}) (n int, err error) {
	return coll.replacer(predicate, newData, false)
}

// ReplaceAll replaces all the matches of the predicate with the newData and returns the
// count of updates.
// Replace is the most costly operation. The library does not provide a method to update parts of a
// document since document is not known to the system.
func (coll *Coll) ReplaceAll(predicate QueryPredicate, newData interface{}) (n int, err error) {
	return coll.replacer(predicate, newData, true)
}

// UpdateFirst updates the first match of predicate in place with the data provided by the
// updateFunction
func (coll *Coll) UpdateFirst(predicate QueryPredicate, updateFunction UpdateFunc) (n int, err error) {
	return coll.updater(predicate, updateFunction, false)
}

// UpdateAll updates all the matches of the predicate in place with the data provided by the
// updateFunction
func (coll *Coll) UpdateAll(predicate QueryPredicate, updateFunction UpdateFunc) (n int, err error) {
	return coll.updater(predicate, updateFunction, true)
}

func (coll *Coll) updater(pred QueryPredicate, uf UpdateFunc, updateAll bool) (n int, err error) {
	chunks, err := coll.getChunks()
	n = 0
	if err != nil {
		return n, err // hiç update yok
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return n, nil
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	var data RecordInstance
	var bufferStore = make([]byte, 2*1024*1024) // 2 mb buffer
	buffer := bytes.NewBuffer(bufferStore)

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return n, err
		}

		scn := bufio.NewScanner(f)
		buffer.Reset()
		predicateMatched := false
		anyMatchesOccured := false

		// chunk verisi taranır ve bütün kayıtlar mem buffer içine yazılır.
		// Bu durumda kayıt değişikliği yerinde yapılır.
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				predicateMatched = false
			} else {
				_ = json.Unmarshal(line, &data) // TODO: Handle error
				predicateMatched = pred(data)

				if predicateMatched && (!anyMatchesOccured || updateAll) {
					newData := uf(&data)
					newDataBytes, err := json.Marshal(newData)
					if err != nil {
						panic(fmt.Sprintf("updateFunction result cannot be marshalled: %s", err.Error()))
					}
					buffer.Write(newDataBytes)
				} else {
					buffer.Write(line)
				}
			}

			buffer.WriteString(recordSepStr)
			anyMatchesOccured = anyMatchesOccured || predicateMatched
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
		if anyMatchesOccured {
			// Kayıt bir dosyada bulunmuş ve silinmiş demektir.
			// Bu durumda buffer, işlem yapılan chunk üzerine yazılır.
			f, err = os.Create(chunkPath) // Truncate file
			if err != nil {
				return n, err
			}
			//_, err := f.Write(bufferStore)
			_, err = buffer.WriteTo(f)
			if err != nil {
				// yazma hatası
				return n, err
			}
			_ = f.Close()
			n++ // bu aşamada veri commit olmuş, değişiklik gerçekleşmiştir.
			f = nil
			break // Chunk loop kır.
		}
	} //end chunks

	return n, nil
}

func (coll *Coll) replacer(pred QueryPredicate, nData interface{}, replaceAll bool) (n int, err error) {
	chunks, err := coll.getChunks()
	n = 0
	if err != nil {
		return n, err // hiç update yok
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return n, nil
	}

	// Yeni kayıt kontrol edilir
	newDataBytes, err := json.Marshal(nData)
	if err != nil {
		// Yeni kayıt dönüştürülemiyor demektir.
		return n, err
	}

	var f *os.File
	// Burada predicate içinde oluşabilecek olan hatayı yakalarız.
	// Hata olursa isimli return value'ları buna göre düzenleriz.
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("predicate error: %s", r.(error).Error()))
			if f != nil { // dosya kapanmamışsa kapat
				_ = f.Close()
			}
		}
	}()

	var data RecordInstance
	var bufferStore = make([]byte, 2*1024*1024) // 2 mb buffer
	buffer := bytes.NewBuffer(bufferStore)

	for _, chunk := range chunks {
		// Veri aranır. Bunun için bütün chunklara bakılır
		chunkPath := filepath.Join(coll.dbpath, chunk.Name())
		f, err = os.Open(chunkPath)
		if err != nil {
			return n, err
		}

		scn := bufio.NewScanner(f)
		buffer.Reset()
		predicateMatched := false
		anyMatchesOccured := false

		// chunk verisi taranır ve bütün kayıtlar mem buffer içine yazılır.
		// Bu durumda kayıt değişikliği yerinde yapılır.
		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				predicateMatched = false
			} else {
				_ = json.Unmarshal(line, &data) // TODO: Handle error
				predicateMatched = pred(data)

				if predicateMatched && (!anyMatchesOccured || replaceAll) {
					// Eğer daha önce bir değişiklik olmamışsa ve predicate eşleme yaptıysa
					// yani ilk defa bir eşleme gerçekleşiyorsa...
					buffer.Write(newDataBytes)
				} else {
					buffer.Write(line)
				}
			}

			buffer.WriteString(recordSepStr)
			anyMatchesOccured = anyMatchesOccured || predicateMatched
		}
		_ = f.Close() // TODO: Handle error
		f = nil       // temizle
		if anyMatchesOccured {
			// Kayıt bir dosyada bulunmuş ve silinmiş demektir.
			// Bu durumda buffer, işlem yapılan chunk üzerine yazılır.
			f, err = os.Create(chunkPath) // Truncate file
			if err != nil {
				return n, err
			}
			//_, err := f.Write(bufferStore)
			_, err = buffer.WriteTo(f)
			if err != nil {
				// yazma hatası
				return n, err
			}
			_ = f.Close()
			n++ // bu aşamada veri commit olmuş, değişiklik gerçekleşmiştir.
			f = nil
			break // Chunk loop kır.
		}
	} //end chunks

	return n, nil
}

// createChunk Creates a new chunk for storing data
func (coll *Coll) createChunk() (*fs.FileInfo, error) {
	lastChunk, _ := coll.getLastChunk()
	if lastChunk == nil {
		// Diskte başka chunk yok
		chunkPath := filepath.Join(coll.dbpath, firstChunkName)
		f, err := os.Create(chunkPath)
		if err != nil {
			// Dosya oluşturmada hata
			return nil, err
		}
		fstat, _ := f.Stat()
		defer f.Close()
		return &fstat, nil
	}

	// lastChunk var. Bu durumda dosya boyutu kontrol edilir. Eğer maxChunkSize'dan büyükse yeni bir chunk yapılır.
	if (*lastChunk).Size() > maxChunkSize {
		// yeni bir chunk yap
		chunkNrStr := strings.Split((*lastChunk).Name(), ".")[0]
		chunkNr, err := strconv.ParseUint(chunkNrStr, 16, 32)
		if err != nil {
			//dosya adı ile ilgili bir problem
			return nil, errors.New(fmt.Sprintf("cannot get chunk nr: %s", err.Error()))
		}
		//yeni chunk yap
		chunkNr += 1
		newChunkName := fmt.Sprintf("%02x.json", chunkNr)
		chunkPath := filepath.Join(coll.dbpath, newChunkName)
		f, err := os.Create(chunkPath)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("cannot create chunk: %s", err.Error()))
		}
		fstat, _ := f.Stat()
		defer f.Close()
		lastChunk = &fstat
	}

	// Kontroller tamam. Bu chunk kullanılabilir.
	return lastChunk, nil
}

// getChunks checks disk storage and returns the chunk files if any.
func (coll *Coll) getChunks() ([]fs.FileInfo, error) {
	fileElements, err := ioutil.ReadDir(coll.dbpath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot read chunks: %s", err.Error()))
	}

	// Dosya adları kontrol edilir.
	reFileName, _ := regexp.Compile("^[\\da-fA-F]{2}\\.json$")
	resultArray := make([]fs.FileInfo, len(fileElements))
	idx := 0
	for _, finfo := range fileElements {
		if !finfo.IsDir() {
			if reFileName.MatchString(finfo.Name()) {
				resultArray[idx] = finfo
				idx += 1
			}
		}
	}

	return resultArray[:idx], nil
}

// getLastChunk returns a chunk to store data if there are any.
func (coll *Coll) getLastChunk() (*fs.FileInfo, error) {
	chunks, err := ioutil.ReadDir(coll.dbpath)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot read chunks: %s", err.Error()))
	}

	var lastChunk *fs.FileInfo = nil
	reFileName, _ := regexp.Compile("^[\\da-fA-F]{2}\\.json$")

	for _, finfo := range chunks {
		if !finfo.IsDir() {
			//aradığımız dosya
			if reFileName.MatchString(finfo.Name()) {
				lastChunk = &finfo
			}
		}
	}

	return lastChunk, nil
}
