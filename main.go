package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
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

var (
	DBS = map[string]string{}
	dbs = map[string]*sql.DB{}
)

func init() {
	log.SetFlags(log.Flags() | log.Lshortfile)
	bytes, err := ioutil.ReadFile("config.conf")
	e(err, true)
	str := string(bytes)
	lines := strings.Split(str, "\n")
	reg, err := regexp.Compile(`\s+`)
	e(err, true)
	for num, line := range lines {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := reg.Split(line, -1)
		if len(fields) != 2 {
			log.Printf("Line %d is not match the config format.", num+1)
			continue
		}
		dbname := fields[0]
		dburi := fields[1]
		DBS[dbname] = dburi
		dbs[dbname] = init_db(dburi)
	}
}

func init_db(uri string) *sql.DB {
	db, err := sql.Open("mysql", uri)
	e(err, true)
	return db
}

func task(dbname string, db *sql.DB) {
	for {
		rows, err := db.Query("select id,info,time from information_schema.PROCESSLIST where time > 5 and command = 'Query';")
		if err != nil {
			log.Println(err.Error())
			dbs[dbname] = init_db(DBS[dbname])
			task(dbname, dbs[dbname])
			break
		}
		for rows.Next() {
			var id string
			var info string
			var time string
			err = rows.Scan(&id, &info, &time)
			if err != nil {
				log.Println(err.Error())
			} else {
				log.Printf("[FOUND SQL]: '%s'@'%s' (ConnectionId: %s, Time: %s)", info, dbname, id, time)
				_, err := db.Exec(fmt.Sprintf("kill %s;", id))
				if err != nil {
					log.Println(err.Error())
				} else {
					log.Printf("[KILLED SQL]: '%s'@'%s' (ConnectionId: %s, Time: %s)", info, dbname, id, time)
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func main() {
	for dbname, db := range dbs {
		go task(dbname, db)
	}
	c := make(chan int)
	log.Println(<-c)
}
