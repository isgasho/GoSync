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
func NewDBSync(from *DBS, to ...*DBS) (obj *DBSync) {
	var c bool = true
	if err := from.Ping(); err != nil {
		log.Println("From DB Connect Failed ", err)
		c = false
	}
	for i, db := range to {
		if err := db.Ping(); err != nil {
			log.Printf("[%d] To DB Connect Failed. %s\n", i, err)
			c = false
		}
	}
	if !c {
		log.Fatalln("Init Sync Failed,Exit.")
	}
	dbinfo := make([]*DBS, len(to))
	copy(dbinfo, to)
	// dbs := DBS{from, SyncData{}}
	return &DBSync{from, dbinfo, make([]Condition, 0, 10), &sync.WaitGroup{}}
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
		ordcom = " "
	}
	return
}
func (db *DBSync) count() (count int) {
	condi, ordcom := db.getCondition()
	coustr := fmt.Sprintf("SELECT COUNT(*) FROM %s.`%s` %s %s ", db.soudb.db, db.soudb.tn, condi, ordcom)
	err := db.soudb.QueryRow(coustr).Scan(&count)
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	return count
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
			for i, v := range db.destdb {
				cols := strings.Join(v.cols, ",")
				s := fmt.Sprintf("INSERT INTO %s.`%s` (%s) VALUES (%s) ", v.db, v.tn, cols, vlustr)
				re, err := v.Exec(s)
				if err != nil {
					log.Println("Insert Error ", err)
				}
				log.Println(re)

				log.Println(i, s, d)
			}
		}
	}
}
func (db *DBSync) product(i, step int) {
	condi, ordcom := db.getCondition()
	cols := strings.Join(db.soudb.cols, ",")

	selstr := fmt.Sprintf("SELECT %s FROM %s.`%s` %s %s LIMIT %d,%d", cols, db.soudb.db, db.soudb.tn, condi, ordcom, i, step)
	rows, err := db.soudb.Query(selstr)
	defer rows.Close()
	if err != nil {
		log.Fatalln("Count Error ", err)
		return
	}
	n := len(db.soudb.cols)

	values := make([]sql.RawBytes, n)
	scanArgs := make([]interface{}, n)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// var smap = make(map[string]interface{})
	ch := make(chan []sql.RawBytes)
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
	db.wait.Done()
}

func (db *DBSync) Start(step int) {
	// ctx, cancel := context.WithCancel(context.Background())
	count := db.count()
	slice := count / step
	for i := 0; i < slice+1; i++ {
		go db.product(i*step, step)
		db.wait.Add(1)
	}
	db.wait.Wait()
}
