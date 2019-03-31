package main

import (
	"fmt"

	"github.com/tonyupup/GoSync/lib"
)

func main() {
	lib.Init()
	frdb := lib.NewDBS("root:admin@tcp(127.0.0.1:3306)/T", "T", "tb1", "name")
	todb := lib.NewDBS("root:admin@tcp(127.0.0.1:3306)/T", "T", "tb", "name")
	// todb1 := lib.NewDBS("root:rootpwd@tcp(127.0.0.1:3306)/T", "T", "private2", "price", "unit_price", "owner", "phone", "area")

	// todb1 := lib.NewDBS("root:rootpwd@tcp(127.0.0.1:13306)/house", "house", "private1", "id", "owner", "phone")
	defer frdb.Close()
	defer todb.Close()
	if db, ok := lib.NewDBSync(frdb, todb); !ok {
		fmt.Println("Init Error")
	} else {
		db.Check()
		db.Start(100)
	}

	// db.AddCondition("name != null")

}
