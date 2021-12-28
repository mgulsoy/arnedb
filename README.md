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
    * [Db Management](#db-management) 
    * [Collection Operations And Query](#collection-operations-and-query)

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
### Db Management

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
A `baseDir` can contain multiple databases. The database requires no closing operation.

To store documents at first we need to create a collection. To create a collection we use 
`CreateColl` function:

```go
func main() {
    // Open or create a collection 
    ptrDbInstance, err := arnedb.Open("baseDir","databaseName")
    if err != nil {
        panic(err)	
    }

    //Create a collection
    ptrACollection, err := ptrDbInstance.CreateColl("aCollection")
    if err != nil {
        panic(err)
    }
}
```

The `CreateColl` function returns a pointer to a `Coll` struct. This will enable to
interact with the collections created. The `Open` function loads existing collections. 
If we want to delete a collection we can use the `DeleteColl` function.

```go
func main() {
    // ...
    err := ptrDbInstance.DeleteColl("anotherCollection")
    if err != nil {
        panic(err) // Not found or file system error	
    }
}
```

To get a slice (array) of the names of the collections we can use the `GetCollNames` 
function:

```go
func main() {
    // ...
    collNames := ptrDbInstance.GetCollNames()
    if collNames == nil {
        // there is no collection
        fmt.Println("There is no collection")
    }
}
```

To get a collection we use the `GetColl` function:

```go
func main() {
    // ...
    collNames := ptrDbInstance.GetCollNames()
    if collNames == nil {
        // there is no collection
        fmt.Println("There is no collection")
    }
}
```

### Collection Operations And Query

Once we get a collection from db we can add data to it. To add a document to a collection 
we use the `Add` function:

```go
func main() {
    // ...
    _, err := ptrToAColl.Add(someData)
    if err != nil {
        panic(err)
    }
}
```

`Add` function returns `error` if something goes wrong. If no error is returned then adding
is successful.