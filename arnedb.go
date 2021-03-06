// Package arnedb implements database access layer. Using this package one can embed a simple
// database functionality into his/her application. Arnedb is not a high performance database.
// There are a lot of database products which can achieve high performance. Arnedb provides a
// lightweight database implementation which is embeddable in any GO app. Design goals of Arnedb
// are:
// * Low memory usage: Can be run in resource constrained environments
// * Simplicity: Hence the title implies
// * Text file storage: All the data is stored in text based JSON files
package arnedb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Coll represents a single collection of documents. There is no limit for collections
type Coll struct {
	dbpath string // Kolleksiyon klasörünün yolu.
	// Name is the collection name.
	Name string
}

// ArneDB represents a single database. There is no limit for databases. (Unless you have enough disk space)
type ArneDB struct {
	// Name is the database name
	Name    string
	baseDir string           // Veritabanı ana klasörü,
	path    string           // Veritabanı tam yolu
	colls   map[string]*Coll // içindeki Coll'lar (Kolleksiyonlar)
}

// Open function opens an existing or creates a new database.
func Open(baseDir, dbName string) (*ArneDB, error) {

	// baseDir var mı? Yoksa oluştur.
	// TODO: Error tipi oluşturulabilir. Şimdilik sadece metin errorları kullanılır
	bfi, err := os.Stat(baseDir)
	if os.IsNotExist(err) {
		return nil, errors.New(fmt.Sprintf("Basedir does not exist! : %s", err.Error())) //hata, ana klasör yok
	}

	if !bfi.Mode().IsDir() {
		// ana klasör aslında klasör değil
		return nil, errors.New("base dir is not a dir")
	}

	// Ana klasör var, Şimdi veritabanına bakacağız.
	dbPath := filepath.Join(baseDir, dbName)
	dbfi, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		//Eğer yoksa oluştur
		err = os.Mkdir(dbPath, 0700)
		if err != nil {
			// oluşturulamıyor!
			return nil, err
		}
	} else {
		//Aynı adlı dosya olabillir.
		if !dbfi.Mode().IsDir() {
			// Bir klasör değil!
			return nil, errors.New("a file exists with the same name")
		}
	}

	//Kontroller tamam db hazır
	var db = ArneDB{
		baseDir,
		dbName,
		dbPath,
		make(map[string]*Coll),
	}

	// TODO: Veritabanı compact işlemleri yapılması

	// Şimdi (coll) kolleksiyonlar yüklenir.
	files, err := ioutil.ReadDir(dbPath)
	if err != nil {
		return nil, errors.New("cannot read db collections")
	}

	for _, finfo := range files {
		if finfo.IsDir() {
			// Bu bizim ilgilendiğimiz kolleksiyondur
			var c = Coll{
				Name:   finfo.Name(),
				dbpath: filepath.Join(dbPath, finfo.Name()),
			}
			db.colls[c.Name] = &c
		}
		// dosyalar ile ilgilenmeyiz!
	}

	// klasörlerin her biri bizim kolleksiyonumuzdur.

	return &db, nil // hatasız dönüş
}

// TODO: Export işlemi : Zip dosyası olarak export edilir.
// TODO: Import işlemi : Zip dosyası import edilir.

// Herhangi bir şeyi bellekte tutmadığımız için Close gibi bir işleme ihtiyacımız yok.

// Collection İşlemleri ---------------------------------------------------------------

// CreateColl function creates a collection and returns it.
func (db *ArneDB) CreateColl(collName string) (*Coll, error) {
	// Oluşturulmak istenen collection var mı ona bakarız.
	collPath := filepath.Join(db.path, collName)
	_, err := os.Stat(collPath)

	if os.IsExist(err) {
		return nil, errors.New(fmt.Sprintf("a dir name exists with the same name: %s -> %s", collName, err.Error()))
	}

	// Klasör yok demektir.
	err = os.Mkdir(collPath, 0700)
	if err != nil {
		return nil, err
	} // klasörü oluşturamadı

	var c = Coll{
		Name:   collName,
		dbpath: collPath,
	}
	db.colls[c.Name] = &c

	return &c, nil
}

// DeleteColl function deletes a given collection.
func (db *ArneDB) DeleteColl(collName string) error {
	collObj, keyFound := db.colls[collName]
	if !keyFound {
		return errors.New("collection does not exist")
	}

	err := os.RemoveAll(collObj.dbpath)
	if err == nil { // file system removal success
		delete(db.colls, collName)
	}

	// işlem başarılı
	return err
}

// GetColl gets the collection by the given name. It returns the pointer if it finds a collection
// with the given name. If not it returns nil
func (db *ArneDB) GetColl(collName string) *Coll {

	if len(db.colls) == 0 {
		return nil // Return nil if there is no collection
	}

	c, keyExists := db.colls[collName]
	if !keyExists {
		return nil
	}

	return c
}

// GelCollNames returns all present collection names as []string
func (db *ArneDB) GelCollNames() (result []string) {
	if len(db.colls) == 0 {
		return nil // Return nil if there is no collection
	}

	result = make([]string, len(db.colls))
	i := 0
	for k := range db.colls {
		result[i] = k
		i++
	}

	return result
}
