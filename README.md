# ArneDB
ArneDB is an embedded document (NoSql) database. There is no server implementation. 
Just import the package and GO. 

ArneDB is not a high-performance database. There are a lot of database products which 
can achieve high performance. Arnedb provides a lightweight database implementation 
which is embeddable in any GO app. 

The design goals of Arnedb are:

* Low memory usage: Can be run in resource constrained environments
* Simplicity: There are only 10 functions.
* Text file storage: All the data is stored in text based JSON files

### Minimum Go Version

Go 1.17+

## Overview

* [Installation](#installation)
* [Usage](#usage)

# Installation

This module can be installed with the `go get` command:

    go get github.com/mgulsoy/arnedb

This module is pure __GO__ implementation. No external libraries required. Only standart
libraries used.

# Usage

After installing the library, import it into your app file:

```go
import (
    "github.com/mgulsoy/arnedb"
)
```

ArneDB uses a folder to store data. To create or open a database use `Open` function:

```go
func main() {
	ptrDbInstance, err := arnedb.Open("baseDir","databaseName")
	if err != nil {
	    panic(err)	
    }
}
```

The `Open` function checks whether `baseDir` exists and then creates `databaseName` database. 
A `baseDir` can contain multiple databases.