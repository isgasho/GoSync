package lib

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

/* type Async interface{
	Async (*sql.DB)
} */

type SyncData struct {
	tn   string
	cols []string
}
type DBS struct {
	from *sql.DB
	to   []*sql.DB
}

type Condition struct {
	item  string
	mark  string
	value string
}
type DBSync struct {
	DBS
	cols []SyncData
	cond []Condition
	wait *sync.WaitGroup
}

func NewDBS(from string, to ...string) (dbs *DBS) {
	frc, err := sql.Open("mysql", from)
	if err != nil {
		fmt.Println("NewDBS Error ", err)
	}
	odbs := make([]*sql.DB, len(to))
	for i, db := range to {
		toc, err1 := sql.Open("mysql", db)
		odbs[i] = toc
		if err1 != nil {
			fmt.Println(err1)
		}
	}
	return &DBS{frc, odbs}
}
func (db *DBS) Close() {
	db.from.Close()
	for _, v := range db.to {
		v.Close()
	}
}
func NewDBSync(from, to string) (obj *DBSync) {
	db := NewDBS(from, to)
	if db == nil {
		fmt.Println("Error happend NewDBSYnc")
		return nil
	}
	return &DBSync{*db, make([]SyncData, 1, 10), make([]Condition, 0, 10), &sync.WaitGroup{}}
}
func (obj *DBSync) AddFromCols(tn string, cols ...string) {
	sd := SyncData{}
	sd.tn = tn
	sd.cols = cols
	obj.cols[0] = sd
}
func (obj *DBSync) AddToCols(tn string, cols ...string) {
	sd := SyncData{}
	sd.tn = tn
	sd.cols = cols
	obj.cols = append(obj.cols, sd)
}
func (db *DBS) Check() bool {
	if db.from.Ping() != nil {
		return false
	}
	for _, v := range db.to {
		if v.Ping() != nil {
			return false
		}
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
