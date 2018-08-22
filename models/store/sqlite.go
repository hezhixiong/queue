package store

import (
	"database/sql"
	"fmt"
	"os"
	"path"

	"hezhixiong.im/share/lib"
	"hezhixiong.im/share/models"

	_ "github.com/mattn/go-sqlite3"
)

var (
	db *sql.DB
)

type Message struct {
	ID          int64
	AccountGuid string
	QueueName   string
	Message     string
}

type Sqlite struct {
	Db        *sql.DB
	TableName string
}

// --------------------------------------------------------------------------------

func Init(driverName string, dbName string) error {
	var err error

	if db == nil {
		if db, err = sql.Open(driverName, dbName); err != nil {
			return err
		}
	}

	if !lib.IsFileExist(dbName) {
		if err = os.MkdirAll(path.Dir(dbName), 0644); err == nil {
			if _, err = os.Create(dbName); err == nil {
				return NewSqlite().createTable()
			}
		}
	}

	return err
}

func NewSqlite() *Sqlite {
	return &Sqlite{
		Db:        db,
		TableName: models.TABLE_QUEUE_MESSAGE,
	}
}

func (this *Sqlite) Insert(accountGuid string, queueName string, message []byte) (int64, error) {
	cmd := fmt.Sprintf("INSERT INTO %s(AccountGuid, QueueName, Message) VALUES(?, ?, ?)", this.TableName)
	stmt, err := this.Db.Prepare(cmd)
	if err != nil {
		return 0, err
	}

	ret, err := stmt.Exec(accountGuid, queueName, message)
	if err != nil {
		return 0, err
	}

	return ret.LastInsertId()
}

func (this *Sqlite) GetLimits(accountGuid string, queueName string, limit uint64) ([]*Message, error) {
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE (AccountGuid=\"%s\") AND (QueueName=\"%s\") "+
		" ORDER BY ID DESC LIMIT %d", this.TableName, accountGuid, queueName, limit)

	rows, err := this.Db.Query(cmd)
	if err != nil {
		return nil, err
	}

	var data = make([]*Message, 0, 100)

	for rows.Next() {
		m := new(Message)
		if err := rows.Scan(&m.ID, &m.AccountGuid, &m.QueueName, &m.Message); err != nil {
			return nil, err
		}

		data = append(data, m)
	}

	return data, nil
}

func (this *Sqlite) DeleteByIds(ids string) (int64, error) {
	cmd := fmt.Sprintf("DELETE FROM %s WHERE ID IN (%s)", this.TableName, ids)

	ret, err := this.Db.Exec(cmd)
	if err != nil {
		return 0, err
	}

	return ret.RowsAffected()
}

// --------------------------------------------------------------------------------

func (this *Sqlite) createTable() error {
	cmd := fmt.Sprintf("CREATE TABLE %v ("+
		"ID INTEGER PRIMARY KEY AUTOINCREMENT,"+
		"AccountGuid VARCHAR(64) NULL,"+
		"QueueName VARCHAR(64) NULL,"+
		"Message TEXT NULL);", this.TableName)

	_, err := this.Db.Exec(cmd)
	return err
}
