package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
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

type Tables struct {
	Items []string `json:"tables"`
}

type Records struct {
	Items []interface{} `json:"records"`
}

func NewDbExplorer(db *sql.DB) (*MyApi, error) {
	return &MyApi{
		DB: db,
	}, nil
}

func (srv *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "GET":
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

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func (dbExplorer *MyApi) List(w http.ResponseWriter, r *http.Request) {

	var res interface{}
	var err error

	res, err = dbExplorer.ListAllTables()

	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	tableName := strings.Split(r.URL.Path, "/")[1]

	if tableName == "/" {
		makeOutput(w, ApiResponse{
			Response: &res,
		}, http.StatusOK)
		return
	}

	if _, ok := Find(res.(*Tables).Items, tableName); !ok {
		makeOutput(w, ApiResponse{
			Error: "unknown table",
		}, http.StatusNotFound)
		return
	}

	//switch r.URL.Path {
	//	case "/items":
	//		res, err = dbExplorer.ListTableByName("items")
	//}

	var limitIntVal, offsetIntVal, idIntVal int

	limit := r.URL.Query().Get("limit")
	if limit == "" {
		limitIntVal = 5
	} else {
		limitIntVal, err = strconv.Atoi(limit)
	}
	offset := r.URL.Query().Get("offset")
	if offset == "" {
		offsetIntVal = 0
	} else {
		offsetIntVal, err = strconv.Atoi(offset)
	}

	id := path.Base(r.URL.Path)
	if id == "" || id == "/" {
		res, err = dbExplorer.ListTableByName(tableName, limitIntVal, offsetIntVal)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	} else {
		idIntVal, err = strconv.Atoi(id)
		res, err = dbExplorer.ListTableByNameAndId(tableName, idIntVal)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}

	makeOutput(w, ApiResponse{
		Response: &res,
	}, http.StatusOK)
}

func (dbExplorer *MyApi) ListAllTables() (*Tables, error) {
	items := []string{}

	rows, err := dbExplorer.DB.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}

	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	for rows.Next() {
		var item string
		err = rows.Scan(&item)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return &Tables{Items: items}, nil
}

func (dbExplorer *MyApi) ListTableByName(tableName string, limit int, offset int) (interface{}, error) {
	rows, err := dbExplorer.DB.Query(fmt.Sprintf("SELECT * FROM %s LIMIT ?, ?", tableName), offset, limit)
	if err != nil {
		return nil, err
	}

	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	var items []interface{}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var vals = make(map[string]interface{})
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var value interface{}
		for i, col := range values {
			if col != nil {
				//valueOf := reflect.ValueOf(col)
				//fmt.Println(valueOf)
				//fmt.Println(valueOf.Kind())
				switch columnTypes[i].DatabaseTypeName() {
				case "INT":
					value, _ = strconv.Atoi(string(col))
				case "VARCHAR", "TEXT":
					value = string(col)
				}
				//value = col
			} else {
				value = nil
			}
			fmt.Println(columns[i], ": ", value)
			vals[columns[i]] = value
		}
		items = append(items, vals)
		fmt.Println("-----------------------------------")
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	//colNames, err := rows.Columns()
	//if err != nil {
	//	return nil, err
	//}
	//
	//var vals = make(map[string]interface{})
	//cols := make([]interface{}, len(colNames))
	////colPtrs := make([]interface{}, len(colNames))
	////for i := 0; i < len(colNames); i++ {
	////	colPtrs[i] = &cols[i]
	////}
	//for i, _ := range colNames {
	//	cols[i] = new(sql.RawBytes)
	//}
	//
	//for rows.Next() {
	//	err = rows.Scan(cols...)
	//	if err != nil {
	//		return nil, err
	//	}
	//	for i, col := range cols {
	//		vals[colNames[i]] = col
	//	}
	//	// Do something with the map
	//	for key, val := range vals {
	//		fmt.Println("Key:", key, "Value Type:", reflect.TypeOf(val), "Value: ", val)
	//	}
	//}

	//var values [][]interface{}
	//if rows.Next() {
	//	columns, err := rows.ColumnTypes()
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	values := make([]interface{}, len(columns))
	//	object := map[string]interface{}{}
	//
	//	for i, column := range columns {
	//		//switch column.DatabaseTypeName() {
	//		//	case "INT": object[column.Name()] = reflect.New(column.ScanType()).Interface().(*int32)
	//		//	case "VARCHAR", "TEXT": object[column.Name()] = reflect.New(column.ScanType()).Interface().(*sql.RawBytes)
	//		//}
	//		object[column.Name()] = reflect.New(column.ScanType()).Interface().(*sql.RawBytes)
	//		values[i] = object[column.Name()]
	//	}
	//
	//	err = rows.Scan(values...)
	//	if err != nil {
	//		return nil, err
	//	}
	//}
	return &Records{Items: items}, nil
}

func (dbExplorer *MyApi) ListTableByNameAndId(tableName string, id int) (interface{}, error) {
	row := dbExplorer.DB.QueryRow(fmt.Sprintf("SELECT * FROM %s WHERE id = ?", tableName), id)

	var items []interface{}

	columns, err := row.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	columnTypes, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}

	if row.Next() {
		var vals = make(map[string]interface{})
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var value interface{}
		for i, col := range values {
			if col != nil {
				//valueOf := reflect.ValueOf(col)
				//fmt.Println(valueOf)
				//fmt.Println(valueOf.Kind())
				switch columnTypes[i].DatabaseTypeName() {
				case "INT":
					value, _ = strconv.Atoi(string(col))
				case "VARCHAR", "TEXT":
					value = string(col)
				}
				//value = col
			} else {
				value = nil
			}
			fmt.Println(columns[i], ": ", value)
			vals[columns[i]] = value
		}
		items = append(items, vals)
		fmt.Println("-----------------------------------")
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return &Records{Items: items}, nil
}

func (dbExplorer *MyApi) Create(w http.ResponseWriter, r *http.Request) {

}

func (dbExplorer *MyApi) Update(w http.ResponseWriter, r *http.Request) {

}

func (dbExplorer *MyApi) Delete(w http.ResponseWriter, r *http.Request) {

}
