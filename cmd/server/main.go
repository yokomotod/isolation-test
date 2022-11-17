package main

import (
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/yokomotod/zakodb/internal/zakodb"
)

var db zakodb.ZakoDB

func handle(w http.ResponseWriter, r *http.Request) error {
	in, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("io.ReadAll: %w", err)
	}

	out, _, err := db.Handle(string(in))
	if err != nil {
		return fmt.Errorf("db.Handle: %w", err)
	}
	if out != nil {
		fmt.Fprintf(w, "%s\n", out)
	}

	return nil
}

func start() error {
	var err error
	db, err = zakodb.NewZakoDB("zakodb1")
	if err != nil {
		return err
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		err := handle(w, r)
		if err != nil {
			http.Error(w, fmt.Sprintf("%+v", err), 500)
		}
	})

	return http.ListenAndServe(":8080", nil)
}

func main() {
	fmt.Fprintln(os.Stderr, start())
	os.Exit(1)
}
