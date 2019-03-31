package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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
	*sql.DB
	SyncData
}

type Condition struct {
	item  string
	mark  string
	value string
}
type DBSync struct {
	soudb  *DBS
	destdb []*DBS
	cond   []Condition
	wait   *sync.WaitGroup
}

func Init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
func NewDBS(from, db, tn string, cols ...string) (dbs *DBS) {
	frc, _ := sql.Open("mysql", from)
	// defer frc.Close()
	return &DBS{frc, SyncData{db, tn, cols}}
}

func NewDBSync(from *DBS, to ...*DBS) (obj *DBSync, ok bool) {
	ok = true
	if err := from.Ping(); err != nil {
		log.Println("From DB Connect Failed ", err)
		ok = false
	}
	// from.SetMaxOpenConns(100)
	for i, db := range to {
		// db.SetMaxOpenConns(100)
		if err := db.Ping(); err != nil {
			log.Printf("[%d] To DB Connect Failed. %s\n", i, err)
			ok = false
		}
	}
	if !ok {
		log.Fatalln("Init Sync Failed,Exit.")
		return nil, ok
	}
	dbinfo := make([]*DBS, len(to))
	copy(dbinfo, to)
	// dbs := DBS{from, SyncData{}}
	return &DBSync{from, dbinfo, make([]Condition, 0, 10), &sync.WaitGroup{}}, ok
}

func (db *DBSync) Check() bool {
	slen := len(db.soudb.cols)
	fail := false
	for _, db := range db.destdb {
		if slen != len(db.cols) {
			fail = true
			log.Println("同步列数量不一致")
		}
	}
	if fail {
		log.Println("检查同步出错.")
		return false
	}
	log.Println("检测正常")
	return true
}

func (db *DBSync) AddCondition(con string) (err error) {
	command := strings.Split(con, " ")
	if len(command) != 3 {
		log.Printf("Condition format error.<%s>", con)
		return errors.New("Conditon format error.")
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
func (db *DBSync) getCondition() (condi, ordcom string) {
	// var condi, ordcom string
	if len(db.cond) != 0 {
		conds := make([]string, len(db.cond))
		order := make([]string, len(db.cond))
		for i, v := range db.cond {
			conds[i] = fmt.Sprintf("%s%s%s", v.item, v.mark, v.value)
			order[i] = v.item
		}
		condi = " WHERE " + strings.Join(conds, " AND ")
		ordcom = " ORDER BY " + strings.Join(order, ",")
	} else {
		condi = ""
		ordcom = ""
	}
	return
}
func (db *DBSync) count() (count, maxConnections int) {
	var stmp string
	condi, ordcom := db.getCondition()
	coustr := fmt.Sprintf("SELECT COUNT(*) FROM %s.`%s` %s %s ", db.soudb.db, db.soudb.tn, condi, ordcom)
	err := db.soudb.QueryRow(coustr).Scan(&count)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	db.soudb.QueryRow("show variables like 'max_connections';").Scan(&stmp, &maxConnections)
	log.Println("Count number :", count)
	return
}

func (db *DBSync) consum(ctx context.Context, ch chan []sql.RawBytes) {
	vlu := make([]string, len(db.soudb.cols))
	for {
		select {
		case <-ctx.Done():
			return
		case d := <-ch:
			for i, s := range d {
				vlu[i] = fmt.Sprintf("\"%s\"", string(s))
			}
			vlustr := strings.Join(vlu, ",")
			for _, v := range db.destdb {
				cols := strings.Join(v.cols, ",")
				s := fmt.Sprintf("INSERT INTO %s.`%s` (%s) VALUES (%s) ", v.db, v.tn, cols, vlustr)
				_, err := v.Exec(s)
				if err != nil {
					log.Println("Insert Error ", err)
				}
			}
		}
	}
}
func (db *DBSync) product(i, step int) bool {
	condi, ordcom := db.getCondition()
	cols := strings.Join(db.soudb.cols, ",")

	selstr := fmt.Sprintf("SELECT %s FROM %s.`%s` %s %s LIMIT %d,%d", cols, db.soudb.db, db.soudb.tn, condi, ordcom, i, step)
	rows, err := db.soudb.Query(selstr) //链接对象
	defer rows.Close()                  //交还链接对象
	if err != nil {
		log.Println("Select Error ", err, selstr)
		return false
	}
	n := len(db.soudb.cols)

	values := make([]sql.RawBytes, n)
	scanArgs := make([]interface{}, n)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// var smap = make(map[string]interface{})
	ch := make(chan []sql.RawBytes, 100)
	ctx, cancel := context.WithCancel(context.Background())
	go db.consum(ctx, ch)

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
		// for i, col := range values {
		// 	smap[columns[i]] = col
		// }
		valuesparse := make([]sql.RawBytes, n)
		copy(valuesparse, values)
		ch <- valuesparse

	}
	cancel()
	log.Println("End ", i+step)
	db.wait.Done()
	return true
}
func (db *DBSync) close() {
	db.soudb.Close()
	for i := 0; i < len(db.destdb); i++ {
		db.destdb[i].Close()
	}
}
func (db *DBSync) Start(step int) {
	/*
		优化连接数
	*/
	// defer db.close()
	count, max := db.count()
	max = max / 10 * 8
	if step < count/max {
		step = count / max
	}
	slice := count / step
	for i := 0; i < slice+1; i++ {
		go db.product(i*step, step)
		db.wait.Add(1)
	}
	db.wait.Wait()
}
