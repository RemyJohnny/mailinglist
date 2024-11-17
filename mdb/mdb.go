package mdb

import (
	"database/sql"
	"log"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func TryCreate(db *sql.DB) {
	_, err := db.Exec(`
	CREATE TABLE email_lists (
	id INTEGER NOT NULL PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
	email VARCHAR(255) UNIQUE NOT NULL,
	confirmed_at INTEGER,
	opt_out INTEGER
	);
	`)
	if err != nil {
		if sqlError, ok := err.(*pq.Error); ok {
			if sqlError.Code.Name() != "duplicate_table" {
				log.Fatal(sqlError)
			}
		} else {
			log.Fatal(err)
		}
	}
}

func emailEntryFromRow(row *sql.Rows) (*EmailEntry, error) {
	var (
		id          int64
		email       string
		confirmedAt int64
		optOut      bool
	)

	err := row.Scan(&id, &email, &confirmedAt, &optOut)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	t := time.Unix(confirmedAt, 0)

	return &EmailEntry{
		Id:          id,
		Email:       email,
		ConfirmedAt: &t,
		OptOut:      optOut,
	}, nil

}

func CreateEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
	INSERT INTO email_lists(email,confirmed_at,opt_out)
	VALUES($1, 0, 0);`, email)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`
	SELECT id, email,confirmed_at,opt_out
	FROM email_lists
	WHERE email = $1`, email)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		return emailEntryFromRow(rows)
	}
	return nil, nil
}

func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	/* now := time.Now()
	entry.ConfirmedAt = &now */
	t := entry.ConfirmedAt.Unix()

	optOut := 0
	if entry.OptOut {
		optOut = 1
	}

	_, err := db.Exec(`
	INSERT INTO email_lists(email,confirmed_at,opt_out) VALUES($1,$2,$3)
	ON CONFLICT(email) DO UPDATE SET confirmed_at = $4, opt_out = $5
	`, entry.Email, t, optOut, t, optOut)

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func DeleteEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
	UPDATE email_lists
	SET opt_out=1
	WHERE email=$1`, email)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

type GetEmailBatchQueryParams struct {
	Page  int
	Count int
}

func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	var empty []EmailEntry

	rows, err := db.Query(`
	SELECT id, email, confirmed_at, opt_out
	FROM email_lists
	WHERE opt_out = 0
	ORDER BY id ASC
	LIMIT $1 OFFSET $2
	`, params.Count, (params.Page-1)*params.Count)

	if err != nil {
		log.Println(err)
		return empty, err
	}
	defer rows.Close()

	emails := make([]EmailEntry, 0, params.Count)

	for rows.Next() {
		email, err := emailEntryFromRow(rows)
		if err != nil {
			return nil, err
		}
		emails = append(emails, *email)
	}
	return emails, nil
}
