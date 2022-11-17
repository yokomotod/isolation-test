package common

type ZakoDB interface {
	Handle(query string) (rows [][]any, columns []string, err error)
}
