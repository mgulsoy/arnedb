package arnedb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
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

// RecordInstance A record instance read from data file
type RecordInstance map[string]interface{}

// QueryPredicate a function type receiving row instance and returning bool
type QueryPredicate func(instance RecordInstance) bool

// Add Appends data into a collection
func (coll *Coll) Add(data interface{}) (*Coll, error) {

	// Kolleksiyonlar chunkXX.json adı verilen yığınlara ayrılır. Her bir yığın max 1 MB büyüklüğe kadar
	// büyüyebilir.

	// Coll var mı ona bakılır. Yoksa hata...
	_, err := os.Stat(coll.dbpath)
	if os.IsNotExist(err) {
		return coll, errors.New(fmt.Sprintf("collection does not exist: %s", err.Error()))
	}

	// Coll var. En son chunk bulunur.
	lastChunk, err := coll.createChunk()
	if err != nil {
		return coll, err
	}

	// Elimizde en son chunk var.
	chunkPath := filepath.Join(coll.dbpath, (*lastChunk).Name())
	f, err := os.OpenFile(chunkPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return coll, errors.New(fmt.Sprintf("cannot open chunk to add data: %s", err.Error()))
	}

	payload, err := json.Marshal(data)
	if err != nil {
		// veriyi paketlemekte sorun
		return coll, errors.New(fmt.Sprintf("cannot marshal data: %s", err.Error()))
	}

	// Kayıt sonu karakteri eklenir
	payload = append(payload, byte(recordSepChar))
	write, err := f.Write(payload)
	if err != nil {
		return coll, errors.New(fmt.Sprintf("cannot append chunk: %s", err.Error()))
	}

	if write != len(payload) {
		return coll, errors.New(fmt.Sprintf("append to chunk failed with: %d bytes diff", len(payload)-write))
	}
	err = f.Close()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("append failed to clos file: %s", err.Error()))
	}

	// işlem başarılı
	return coll, nil
}

// GetFirst Queries and gets the first occurence of the query
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

// GetAll Queries and gets all instances of the query
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

// DeleteFirst Deletes the first occurence of the predicate result
func (coll *Coll) DeleteFirst(predicate QueryPredicate) error {
	chunks, err := coll.getChunks()
	if err != nil {
		return err
	}

	if len(chunks) == 0 {
		// İçeride hiç veri yok
		return nil
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
			return err
		}

		scn := bufio.NewScanner(f)
		buffer.Reset()
		dataMatched := false
		anyMatchesOccured := false

		for scn.Scan() {
			line := scn.Bytes()
			if len(line) == 0 {
				continue
			}
			_ = json.Unmarshal(line, &data) // TODO: Handle error
			dataMatched = predicate(data)
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
				return err
			}
			//_, err := f.Write(bufferStore)
			_, err := buffer.WriteTo(f)
			if err != nil {
				// yazma hatası
				return err
			}
			_ = f.Close()
			f = nil
			break // Chunk loop kır.
		}
	} //end chunks

	return nil
}

// DeleteAll Deletes all matches of the predicate
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
				continue
			}
			_ = json.Unmarshal(line, &data) // TODO: Handle error
			dataMatched = predicate(data)
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

// getLastChunk Returns a chunk to store data if there are any.
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
