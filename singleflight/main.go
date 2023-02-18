package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/acoshift/pgsql"
	"github.com/acoshift/pgsql/pgctx"
	"golang.org/x/sync/singleflight"
)

func main() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("can not open db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(30)

	// migrate
	_, err = db.Exec(`
		create table if not exists features (
		    name varchar,
		    active boolean,
		    primary key (name)
		);
		insert into features (name, active) values ('f', true) on conflict (name) do nothing;
	`)
	if err != nil {
		log.Fatalf("can not migrate: %v", err)
	}

	err = startUpdateFeatureActiveCache(pgctx.NewContext(context.Background(), db))
	if err != nil {
		log.Fatalf("can not start update feature active cache: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/f0", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/f1", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		err := ensureFeatureActive(ctx, "f")
		if errors.Is(err, featureInactive) {
			w.Write([]byte("feature is not active"))
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/f2", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		err := ensureFeatureActiveWithSingleFlight(ctx, "f")
		if errors.Is(err, featureInactive) {
			w.Write([]byte("feature is not active"))
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/f3", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		err := ensureFeatureActiveWithCache(ctx, "f")
		if errors.Is(err, featureInactive) {
			w.Write([]byte("feature is not active"))
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("ok"))
	})

	addr := "127.0.0.1:8080"
	log.Printf("start web server at %s", addr)
	err = http.ListenAndServe(addr, pgctx.Middleware(db)(mux))
	if err != nil {
		log.Fatalf("can not start web server: %v", err)
	}
}

var featureInactive = errors.New("feature is not active")

func ensureFeatureActive(ctx context.Context, feature string) error {
	active, err := isFeatureActive(ctx, feature)
	if err != nil {
		return err
	}
	if !active {
		return featureInactive
	}
	return nil
}

var featureActiveSF singleflight.Group

func ensureFeatureActiveWithSingleFlight(ctx context.Context, feature string) error {
	active, err, _ := featureActiveSF.Do(feature, func() (any, error) {
		return isFeatureActive(ctx, feature)
	})
	if err != nil {
		return err
	}
	if !active.(bool) {
		return featureInactive
	}
	return nil
}

func isFeatureActive(ctx context.Context, feature string) (bool, error) {
	var active bool
	err := pgctx.QueryRow(ctx, `
		select active
		from features
		where name = $1
	`, feature).Scan(&active)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return active, nil
}

var featureActiveCache struct {
	sync.RWMutex
	m map[string]bool
}

func startUpdateFeatureActiveCache(ctx context.Context) error {
	err := updateFeatureActiveCache(ctx)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
				err := updateFeatureActiveCache(ctx)
				if err != nil {
					log.Printf("can not update feature active cache: %v", err)
				}
			}
		}
	}()
	return nil
}

func updateFeatureActiveCache(ctx context.Context) error {
	m := make(map[string]bool)
	err := pgctx.Iter(ctx, func(scan pgsql.Scanner) error {
		var (
			name   string
			active bool
		)
		err := scan(&name, &active)
		if err != nil {
			return err
		}
		m[name] = active
		return nil
	}, `
		select name, active
		from features
	`)
	if err != nil {
		return err
	}

	featureActiveCache.Lock()
	featureActiveCache.m = m
	featureActiveCache.Unlock()

	return nil
}

func ensureFeatureActiveWithCache(ctx context.Context, feature string) error {
	featureActiveCache.RLock()
	active := featureActiveCache.m[feature]
	featureActiveCache.RUnlock()

	if !active {
		return featureInactive
	}
	return nil
}
