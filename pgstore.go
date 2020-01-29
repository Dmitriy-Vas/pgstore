package pgstore

import (
	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"log"
	"time"
)

// PgStore represents the session store.
type PgStore struct {
	db          *pg.Conn
	stopCleanup chan bool
}

// Session represents the session table.
type Session struct {
	tableName struct{}  `pg:"sessions"`
	Token     string    `pg:",pk"`
	Data      []byte    `pg:",notnull"`
	Expiry    time.Time `pg:",notnull"`
}

func New(db *pg.Conn) *PgStore {
	return NewWithCleanupInterval(db, 5*time.Minute)
}

func NewWithCleanupInterval(db *pg.Conn, cleanupInterval time.Duration) *PgStore {
	p := &PgStore{db: db}
	if cleanupInterval > 0 {
		go p.startCleanup(cleanupInterval)
	}
	return p
}

func (p *PgStore) createTable() error {
	return p.db.CreateTable((*Session)(nil), &orm.CreateTableOptions{
		IfNotExists: true,
	})
}

func (p *PgStore) Find(token string) (b []byte, exists bool, err error) {
	session := new(Session)
	err = p.db.Model(session).
		Where("token = ?", token).
		Where("expiry < ?", time.Now()).
		Select(session.Data)
	if err == pg.ErrNoRows {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (p *PgStore) Commit(token string, b []byte, expiry time.Time) error {
	session := &Session{
		Token:  token,
		Data:   b,
		Expiry: expiry,
	}
	_, err := p.db.Model(session).
		OnConflict("(token) DO UPDATE").
		Set("data = EXCLUDED.data").
		Set("expiry = EXCLUDED.expiry").
		Insert()
	return err
}

func (p *PgStore) Delete(token string) error {
	session := new(Session)
	_, err := p.db.Model(session).
		Where("token = ?", token).
		Delete()
	return err
}

func (p *PgStore) startCleanup(interval time.Duration) {
	p.stopCleanup = make(chan bool)
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			err := p.deleteExpired()
			if err != nil {
				log.Println(err)
			}
		case <-p.stopCleanup:
			ticker.Stop()
			return
		}
	}
}

func (p *PgStore) deleteExpired() error {
	var sessions []Session
	_, err := p.db.Model(&sessions).
		Where("expiry < ?", time.Now()).
		Delete()
	return err
}
