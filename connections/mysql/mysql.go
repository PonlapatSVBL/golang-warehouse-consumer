package mysql

import (
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type mysql struct {
	db  *sqlx.DB
	err error
}

var dbInstance *mysql

func NewMysql() *mysql {
	if dbInstance == nil {
		dbInstance = &mysql{}

		host := os.Getenv("DB_HOST")
		port := os.Getenv("DB_PORT")
		user := os.Getenv("DB_USER")
		password := os.Getenv("DB_PASSWORD")
		dbname := os.Getenv("DB_NAME")

		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)

		dbInstance.db, dbInstance.err = sqlx.Connect("mysql", dsn)
		if dbInstance.err != nil {
			log.Fatalln(dbInstance.err)
		}
	}

	return dbInstance
}

func (mysqlClient mysql) SqlGet(dest interface{}, query string) {
	err := mysqlClient.db.Get(dest, query)
	if err != nil {
		log.Fatalln(err)
	}
}

func (mysqlClient mysql) SqlList(dest interface{}, query string) {
	err := mysqlClient.db.Select(dest, query)
	if err != nil {
		log.Fatalln(err)
	}
}
