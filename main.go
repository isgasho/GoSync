package main

import (
	"context"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tonyupup/syncsql/lib"
)

/* func fff() {
	db, err := sql.Open("mysql", "root:rootpwd@tcp(127.0.0.1:13306)/house")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	// fmt.Println(db)
	rows, err := db.Query("SELECT * FROM private ORDER BY pub_time")
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	// Make a slice for the values
	values := make([]string, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Fetch rows
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		// Now do something with the data.
		// Here we just print each column as a string.
		var value string
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			fmt.Println(columns[i], ": ", value)
		}
		fmt.Println("-----------------------------------")
	}
	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
}
*/
func main() {
	// ctx, cancel := context.WithCancel(context.Background())
	// go watch(ctx, "【监控1】")
	// go watch(ctx, "【监控2】")
	// go watch(ctx, "【监控3】")

	// time.Sleep(10 * time.Second)
	// fmt.Println("可以了，通知监控停止")
	// cancel()
	// //为了检测监控过是否停止，如果没有监控输出，就表示停止了
	// time.Sleep(5 * time.Second)
	/* dbs := lib.NewDBS("root:rootpwd@tcp(127.0.0.1:13306)/house", "root:rootpwd@tcp(127.0.0.1:13306)/house", "private", "private")
	// dbs.SD = lib.Init("private", "pub_time", "name")
	fmt.Println(dbs.Check())
	dbs.Start() */
	lib.Init()
	frdb := lib.NewDBS("root:rootpwd@tcp(127.0.0.1:13306)/house", "house", "private1", "id", "owner", "phone")
	todb := lib.NewDBS("root:rootpwd@tcp(127.0.0.1:13306)/house", "house", "private1", "id", "owner", "phone")
	// todb1 := lib.NewDBS("root:rootpwd@tcp(127.0.0.1:13306)/house", "house", "private1", "id", "owner", "phone")

	db := lib.NewDBSync(frdb, todb)
	db.AddCondition("area > 50")
	// db.Check()
	db.Start(100)
}

func watch(ctx context.Context, name string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println(name, "监控退出，停止了...")
			return
		default:
			fmt.Println(name, "goroutine监控中...")
			time.Sleep(2 * time.Second)
		}
	}
}
