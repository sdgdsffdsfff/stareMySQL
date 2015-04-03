package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"time"
)

func e(err error, fatal bool) (ret bool) {
	if err != nil {
		log.Println(err.Error())
		if fatal {
			os.Exit(1)
		}
		return true
	} else {
		return false
	}
}

func init_db(uri string) *sql.DB {
	db, err := sql.Open("mysql", uri)
	e(err, true)
	return db
}

var (
	DBS = map[string]string{
		"db1": "root:root.com@tcp(127.0.0.1:3306)/test?charset=utf8",
		//"db2": "root:root.com@tcp(127.0.0.1:3306)/test?charset=utf8",
	}
	dbs = map[string]*sql.DB{}
)

func task(db *sql.DB) {
	for {
		rows, err := db.Query("select id,info from information_schema.PROCESSLIST where time>5;")
		e(err, false)

		for rows.Next() {
			var id int
			var sql string
			err = rows.Scan(&id, &sql)
			if !e(err, false) {
				log.Println(id, sql, "is found")
				_, err := db.Exec(fmt.Sprintf("kill %d;", id))
				if !e(err, false) {
					log.Println(id, sql, "is killed")
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}

func main() {
	for dbname, dburi := range DBS {
		dbs[dbname] = init_db(dburi)
	}
	for _, db := range dbs {
		go task(db)
	}
	c := make(chan int)
	log.Println(<-c)
}
