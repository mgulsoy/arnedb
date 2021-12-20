package arnedb

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Coll struct {
	dbpath string // Kolleksiyon klasörünün yolu.
	Name   string // Kolleksiyon adı
}

type ArneDB struct {
	baseDir string           // Veritabanı ana klasörü,
	Name    string           // Veritabanı adı
	path    string           // Veritabanı tam yolu
	colls   map[string]*Coll // içindeki Coll'lar (Kolleksiyonlar)
}

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
