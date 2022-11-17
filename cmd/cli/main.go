package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/yokomotod/zakodb/internal/zakodb0"
)

func cli() error {
	db := zakodb0.NewZakoDB()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("zakodb> ")
		ok := scanner.Scan()
		if !ok {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("scanner.Scan: %w", err)
			}

			fmt.Println("Bye")
			return nil
		}

		in := scanner.Text()
		out, _, err := db.Handle(in)
		if err != nil {
			return fmt.Errorf("db.Handle: %w", err)
		}
		if out != nil {
			fmt.Printf("%s\n", out)
		}
	}
}

func main() {
	err := cli()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
