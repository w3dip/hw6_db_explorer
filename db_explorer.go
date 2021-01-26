package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
type MyApi struct {
	DB *sql.DB
}

type ApiResponse struct {
	Error    string       `json:"error,omitempty"`
	Response *interface{} `json:"response,omitempty"`
}

type AllTables struct {
	Tables []string `json:"tables"`
}

func NewDbExplorer(db *sql.DB) (*MyApi, error) {
	return &MyApi{
		DB: db,
	}, nil
}

func (srv *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {

	case "/":
		srv.List(w, r)

	default:
		makeOutput(w, ApiResponse{
			Error: "unknown method",
		}, http.StatusNotFound)
	}
}

func makeOutput(w http.ResponseWriter, body interface{}, status int) {
	w.WriteHeader(status)
	result, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	_, err_write := io.WriteString(w, string(result))
	if err_write != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
}

func (dbExplorer *MyApi) List(w http.ResponseWriter, r *http.Request) {

	var res interface{}
	res, err := dbExplorer.ListAllTables()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	makeOutput(w, ApiResponse{
		Response: &res,
	}, http.StatusOK)

	// надо закрывать соединение, иначе будет течь
	//defer rows.Close()

	//cols, err := rows.Columns()
	//if err != nil {
	//	http.Error(w, "internal error", http.StatusInternalServerError)
	//	return
	//}
	//vals := make([]interface{}, len(cols))
	//for i, _ := range cols {
	//	vals[i] = new(sql.RawBytes)
	//}
	//
	//for rows.Next() {
	//	err = rows.Scan(vals...)
	//}
}

func (dbExplorer *MyApi) ListAllTables() (*AllTables, error) {
	tables := []string{}

	rows, err := dbExplorer.DB.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}

	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return &AllTables{Tables: tables}, nil
	//res :=
}

func (dbExplorer *MyApi) Create(w http.ResponseWriter, r *http.Request) {

}

func (dbExplorer *MyApi) Update(w http.ResponseWriter, r *http.Request) {

}

func (dbExplorer *MyApi) Delete(w http.ResponseWriter, r *http.Request) {

}
