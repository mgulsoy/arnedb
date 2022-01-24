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
        * [Adding Documents](#adding-documents)
        * [Querying](#querying)
        * [Manipulation](#manipulation)

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

#### Adding Documents

Once we get a collection from db we can add data to it. To add a document to a collection
we use the `Add` function:

```go
func main() {
    // ... 
    
    err := ptrToAColl.Add(someData)
    if err != nil {
        panic(err)
    }
}
```

`Add` function returns `error` if something goes wrong. If no error is returned then adding
is successful.

If we want to add multiple documents at once, we use `AddAll` function.

```go
func main() {
    // ... 
    dataArray := []RecordInstance{data1, data2, data3, data4}
    // This function is a variadic function! Don't forget the ...
    numberOfAdded, err := ptrToAColl.AddAll(dataArray...)
    if err != nil {
        panic(err)
    }
}
```

The function returns the number of added records. This function writes/commits data to disk
at once.

#### Querying

After adding data, we need to query and get the data from the store. There is no special
query language. Query mechanism works with the predicate functions. The operation is similar
to the LINQ. To get a single data we use the `GetFirst` function. This function runs the
predicate and returns the first match in a collection. The predicate function signature
must match the `QueryPredicate` type.

```go
func main() {
    // ... 
    // This predicate checks the records for the id is greater then 34
    queryPredicate := func(instance RecordInstance) bool {
        return instance["id"].(float64) > 34
    }
    
    data, err := ptrToAColl.GetFirst(queryPredicate)
    if err != nil {
        panic(err)
    }
    
    if data == nil {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    }
    
    fmt.Printf("Data: %+v",data) // show data on the console
}
```

The function returns `nil` if there is no match.

If we want to get all the records that the predicate match, we use the `GetAll` function.

```go
func main() {
    // ... 
    // This predicate checks the records for the id is greater then 34
    queryPredicate := func(instance RecordInstance) bool {
        return instance["id"].(float64) > 34
    }
    
    dataSlice, err := ptrToAColl.GetAll(queryPredicate)
    if err != nil {
        panic(err)
    }
    
    if len(dataSlice) == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    }
    
    fmt.Printf("Data: %+v",dataSlice) // show data on the console
}
```

If the predicate does not match any records, the function returns an empty slice.

There is also `GetFirstAsInterface` function. This function tries to return data as a struct
used in the application. This function works a little different with the `GetFirst` function.
Check the example:

```go
type SomeDataType stuct {
	Id              int
	SomeValue       string
	SomeOtherValue  float64
}

func main() {
	// ...
	
    var dataHolder SomeDataType
    var queryPredicate = func(instance interface{}) bool {
        i := instance.(*SomeDataType) // this typecast is required
        return i.Id == 13
    }

    // The holder (3rd) parameter must be an address of a variable
    found, err := ptrToAColl.GetFirstAsInterface(queryPredicate, &dataHolder)
    if err != nil {
        //handle error
        // ...
    }

    if found {
        // data found. You can reach the data with dataHolder
        fmt.Println("Data: ", dataHolder)
        // ...
    } else {
        // Not found, no match
        // if so dataHolder will be nil
        // handle this state ...
    }
}
```

There is also `GetAllAsInterface` function. This function hands the found document to an
argument named `harvestCallback`. This is a callback function. Inside this function you
can harvest the data as you wish. Check the example:

```go
type SomeDataType stuct {
	Id              int
	SomeValue       string
	SomeOtherValue  float64
}

func main() {
	
    // ...
	
    var dataHolder SomeDataType
    var queryPredicate = func(instance interface{}) bool {
        i := instance.(*SomeDataType) // this typecast is required
        return i.Id > 0
    }

    var resultCollection = make([]SomeDataType,0) // create an empty slice
    var harvestCB = func(instance interface{}) bool {
        // this is a double indirection. Please pay attention to the * operators!
        i := *instance.(*SomeDataType) // this typecast is required
        resultCollection = append(resultCollection, i) // harvest as you need
        return true // always return true
    }
	
	// The holder (3rd) parameter must be an address of a variable!
    count, err := ptrToAColl.GetAllAsInterface(queryPredicate, harvestCB, &dataHolder)
    if err != nil {
       //handle error
       // ...
    }
    if count > 0 {
        // query result will be in resultCollection
        fmt.Println("Data: ", resultCollection)
        // ...
    } else {
        // Not found, no match
        // if so resultCollection will be empty
        // handle this state ...
    }
}
```

If you want to get the count of the documents stored, there is the `Count` function. 
Here is an example of how to use it:

```go
func main() {
	
    queryPredicate := func(q RecordInstance) bool {
       return true // we want to count all the records. You can also give conditions here.	
    }
	
    n, err := ptrToAColl.Count(queryPredicate)
    if err != nil {
        // handle error...
    } else {
       // no error
       fmt.Println("Record count:",n)
    }
}
```

#### Manipulation

We can delete records by using `DeleteFirst` and `DeleteAll` functions. The functions accept
a `QueryPredicate` function as an argument and returns the count of deleted records. If the
count is 0 this means no deletion occurred.

```go
func main() {
    // ... 
    // This predicate checks the records for the id is greater then 34
    queryPredicate := func(instance RecordInstance) bool {
        return instance["id"].(float64) > 34
    }
    
    delCount, err := ptrToAColl.DeleteFirst(queryPredicate)
    if err != nil {
        panic(err)
    }
    
    if delCount == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    }
    
    delCount, err = ptrToAColl.DeleteAll(queryPredicate)
    if err != nil {
        panic(err)
    }
    
    if delCount == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    } 
}
```

We can replace or update records by using these functions:

* `ReplaceFirst` : Replaces the first record matched by the query predicate with the given one in place.
* `ReplaceAll` : Replaces all the records matched by the query predicate with the given one in place.
* `UpdateFirst` : Updates the first record matched by the query predicate by using the update function in place.
* `UpdateAll` : Updates all the records matched by the query predicate by using the update function in place.

All these functions return the count of altered records and error. If an error is returned
this means there is a problem with the operation and records are not updated. If the count
returned is 0 then the query predicate matched no record.

```go
func main() {
    // ... 
    // This predicate checks the records for the id is greater then 34
    queryPredicate := func(instance RecordInstance) bool {
        return instance["id"].(float64) > 34
    }
    
    nCount, err := ptrToAColl.ReplaceFirst(queryPredicate, someNewData)
    if err != nil {
        panic(err)
    }
    
    if nCount == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    }
    
    nCount, err = ptrToAColl.ReplaceAll(queryPredicate, someNewData)
    if err != nil {
        panic(err)
    }
    
    if nCount == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    } 
}
```

The **Update** operation accepts an updater function. The function signature must match with
the `UpdateFunc` type.

```go
func main() {
    // ... 
    // This predicate checks the records for the id is greater then 34
    queryPredicate := QueryPredicate(func(instance RecordInstance) bool {
        return instance["id"].(float64) > 34
    })
    
    fUpdt := UpdateFunc(func(ptrRecord *RecordInstance) *RecordInstance {
        (*ptrRecord)["user"] = "Updated First" // change whatever needed to change
        return ptrRecord // and return the result
    })
    
    nCount, err := ptrToAColl.UpdateFirst(queryPredicate, fUpdt)
    if err != nil {
        panic(err)
    }
    
    if nCount == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    }
    
    nCount, err = ptrToAColl.UpdateAll(queryPredicate, fUpdt)
    if err != nil {
        panic(err)
    }
    
    if nCount == 0 {
        // This means the predicate matches no data.
        fmt.Println("No data matched!")
        return
    } 
}
```