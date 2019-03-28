package lib

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

/* type Async interface{
	Async (*sql.DB)
} */

type SyncData struct {
	db, tn string
	cols   []string
}
type DBS struct {
	dbc *sql.DB
	SyncData
}

type Condition struct {
	item  string
	mark  string
	value string
}
type DBSync struct {
	dbinfo []*DBS
	cond   []Condition
	wait   *sync.WaitGroup
}

func Init() {
	log.SetFlags(log.Ldate | log.Lshortfile)
}
func NewDBS(from, db, tn string, cols ...string) (dbs *DBS) {
	frc, err := sql.Open("mysql", from)
	if err != nil {
		log.Fatalln("NewDBS Error ", err)
	}
	return &DBS{frc, SyncData{db, tn, cols}}
}
func NewDBSync(from *DBS, to ...*DBS) (obj *DBSync) {
	var c bool = true
	if err := from.dbc.Ping(); err != nil {
		log.Println("From DB Connect Failed ", err)
		c = false
	}
	for i, db := range to {
		if err := db.dbc.Ping(); err != nil {
			log.Printf("[%d] To DB Connect Failed. %s\n", i, err)
			c = false
		}
	}
	if !c {
		log.Fatalln("Init Sync Failed,Exit.")
	}
	dbinfo := make([]*DBS, len(to)+1)
	dbinfo[0] = from
	dbinfo = append(dbinfo, to...)
	// dbs := DBS{from, SyncData{}}
	return &DBSync{dbinfo, make([]Condition, 0, 10), &sync.WaitGroup{}}
}

func (db *DBSync) Check() bool {
	slen := len(db.dbinfo[0].cols)
	fail := false
	for i, db := range db.dbinfo[1:] {
		if slen != len(db.cols) {
			fail = true
			log.Println("同步列数量不一致")
		}
	}
	if fail {
		log.Println("检查同步出错.")
		return false
	}
	return true
}

func (db *DBSync) AddCondition(con string) (err error) {
	command := strings.Split(con, " ")
	if len(command) != 3 {
		return errors.New("Condition format error.")
	}
	cm := command[1]

	switch cm {
	case ">=":
	case "<=":
	case "==":
	case "=":
	case ">":
	case "<":
	case "!=":
	default:
		return errors.New("Condition format error.")
	}

	db.cond = append(db.cond, Condition{command[0], command[1], command[2]})
	return nil
}
func (db *DBSync) count() int {
	conds := make([]string, 0)
	for _, v := range db.cond {
		conds = append(conds, fmt.Sprintf("%s%s\"%s\"", v.item, v.mark, v.value))
	}
	coustr := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s ORDER BY %s", db.cols[0].tn, strings.Join(conds, " AND "), db.cond[0].item)
	rows, err := db.from.Query(coustr)
	defer rows.Close()
	if err != nil {
		fmt.Println("Count Error ", err)
	}
	values := make([]sql.RawBytes, 1)
	scanArgs := make([]interface{}, 1)
	for i := range values {
		scanArgs[i] = &values[i]
	}
	rows.Next()
	err = rows.Scan(scanArgs...)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	// Now do something with the data.
	// Here we just print each column as a string.
	i, err := strconv.Atoi(string(values[0]))
	if err != nil {
		fmt.Println(err)
	}
	return i
}
func (db *DBSync) consum(data []string) {
	// db.from.Query("SELECT FROM %s WHERE %s ORDER BY %s LIMIT ", args)
	to := db.cols[1:]
	vs := strings.Join(data, ",")
	for i, v := range to {
		cols := strings.Join(v.cols, ",")
		s := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ", v.tn, cols, vs)
		rows, err := db.to[i].Query(s)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(rows)
	}
	db.wait.Done()
}
func (db *DBSync) product(i, step int) {
	conds := make([]string, 0)
	for _, v := range db.cond {
		conds = append(conds, fmt.Sprintf("%s%s\"%s\"", v.item, v.mark, v.value))
	}
	cols := strings.Join(db.cols[0].cols, ",")
	selstr := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d,%d", cols, db.cols[0].tn, strings.Join(conds, " AND "), db.cond[0].item, i, step)
	rows, err := db.DBS.from.Query(selstr)
	defer rows.Close()
	if err != nil {
		fmt.Println("Count Error ", err)
		return
	}
	colums, err1 := rows.Columns()
	if err1 != nil {
		fmt.Println(err1)
		return
	}
	values := make([]sql.RawBytes, len(colums))
	scanArgs := make([]interface{}, len(colums))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		valuesparse := make([]string, len(colums))
		for i, v := range values {
			valuesparse[i] = fmt.Sprintf("\"%s\"", string(v))
		}
		go db.consum(valuesparse)
		db.wait.Add(1)
		// for i, v := range values {
		// 	fmt.Println(colums[i], string(v))
		// }
	}
	db.wait.Done()

}
func (db *DBSync) Start(step int) {
	// ctx, cancel := context.WithCancel(context.Background())
	count := db.count()
	slice := count / step
	for i := 0; i < slice; i++ {
		db.wait.Add(1)
		go db.product(i, step)
	}
	db.wait.Wait()
	db.Close()
}
