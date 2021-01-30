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

type Record struct {
	Item interface{} `json:"record"`
}

type DeleteResult struct {
	Count int64 `json:"deleted"`
}

//type InsertResult struct {
//	Id int64 `json:"id"`
//}

type ColInfo struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default sql.NullString
	Extra   string
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
	case "PUT":
		srv.Create(w, r)
	case "POST":
		srv.Update(w, r)
	case "DELETE":
		srv.Delete(w, r)

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

	//tableName := strings.Split(r.URL.Path, "/")[1]
	tableName := path.Base(r.URL.Path)

	if tableName == "/" {
		makeOutput(w, ApiResponse{
			Response: &res,
		}, http.StatusOK)
		return
	}

	//попробуем достать id
	var idIntVal int
	id := tableName

	idIntVal, err = strconv.Atoi(id)
	if err == nil {
		tableName = strings.Split(r.URL.Path, "/")[1]
	}

	if _, ok := Find(res.(*Tables).Items, tableName); !ok {
		makeOutput(w, ApiResponse{
			Error: "unknown table",
		}, http.StatusNotFound)
		return
	}

	if idIntVal != 0 {
		res, err = dbExplorer.ListTableByNameAndId(tableName, idIntVal)
		if err != nil {
			if err == sql.ErrNoRows {
				makeOutput(w, ApiResponse{
					Error: "record not found",
				}, http.StatusNotFound)
			} else {
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
			return
		}
	} else {
		var limitIntVal, offsetIntVal int

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

		res, err = dbExplorer.ListTableByName(tableName, limitIntVal, offsetIntVal)
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

	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	if err != nil {
		return nil, err
	}

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
				switch columnTypes[i].DatabaseTypeName() {
				case "INT":
					value, _ = strconv.Atoi(string(col))
				case "VARCHAR", "TEXT":
					value = string(col)
				}
			} else {
				value = nil
			}
			fmt.Println(columns[i], ": ", value)
			vals[columns[i]] = value
		}
		items = append(items, vals)
		//fmt.Println("-----------------------------------")
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return &Records{Items: items}, nil
}

func (dbExplorer *MyApi) ListTableByNameAndId(tableName string, id int) (interface{}, error) {

	pk_name, err := dbExplorer.GetPrimaryKeyColumnName(tableName)
	if pk_name == "" {
		return nil, fmt.Errorf("Can't find PRIMARY_KEY")
	}
	if err != nil {
		return nil, err
	}

	rows, err := dbExplorer.DB.Query(fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableName, pk_name), id)

	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	if err != nil {
		return nil, err
	}

	var item interface{}

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

	if rows.Next() {
		var vals = make(map[string]interface{})
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		var value interface{}
		for i, col := range values {
			if col != nil {
				switch columnTypes[i].DatabaseTypeName() {
				case "INT":
					value, _ = strconv.Atoi(string(col))
				case "VARCHAR", "TEXT":
					value = string(col)
				}
			} else {
				value = nil
			}
			//fmt.Println(columns[i], ": ", value)
			vals[columns[i]] = value
		}
		item = vals
		//fmt.Println("-----------------------------------")
	} else {
		return nil, sql.ErrNoRows
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return &Record{Item: item}, nil
}

func (dbExplorer *MyApi) Create(w http.ResponseWriter, r *http.Request) {
	var err error
	//var params map[string]interface{}
	params := make(map[string]interface{})
	tableName := path.Base(r.URL.Path)

	//err = r.ParseForm()
	//if err != nil {
	//	http.Error(w, "internal error", http.StatusInternalServerError)
	//	return
	//}

	//for key, value := range r.Form {
	//	params[key] = value
	//}
	//fmt.Println(r.Form.Get("title"))
	//fmt.Println(r.PostForm.Get("title"))
	//fmt.Println(r.Body)
	//params := r.PostForm.(map[string][]string)
	//data := []byte{}
	//_, err = r.Body.Read(data)
	//if err != nil {
	//	http.Error(w, "internal error", http.StatusInternalServerError)
	//	return
	//}
	//if err := json.Unmarshal(data, &params); err != nil {
	//	http.Error(w, "internal error", http.StatusInternalServerError)
	//	return
	//}
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&params)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	res, err := dbExplorer.Insert(tableName, params)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	//resp := ‘{"affected": ‘ + strconv.Itoa(int(affected)) + ‘}‘
	w.Write([]byte(res))
	//makeOutput(w, ApiResponse{
	//	Response: &res,
	//}, http.StatusOK)
}

func (dbExplorer *MyApi) Insert(tableName string, params map[string]interface{}) (string, error) {
	var err error
	var result sql.Result
	var affected int64
	var id int64

	pk_name, err := dbExplorer.GetPrimaryKeyColumnName(tableName)
	if err != nil {
		return "", err
	}

	var fieldNames []string
	var placeholders []string
	var values []interface{}

	columns := []*ColInfo{}
	rows, err := dbExplorer.DB.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", tableName))

	// надо закрывать соединение, иначе будет течь
	defer rows.Close()

	if err != nil {
		return "", err
	}

	for rows.Next() {
		column := &ColInfo{}
		err = rows.Scan(&column.Field, &column.Type, &column.Null, &column.Key, &column.Default, &column.Extra)
		if err != nil {
			return "", err
		}
		columns = append(columns, column)
	}

	for _, column := range columns {
		if column.Field != pk_name {
			if _, ok := params[column.Field]; ok {
				fieldNames = append(fieldNames, column.Field)
				placeholders = append(placeholders, "?")
				values = append(values, params[column.Field])
			} else if column.Null == "NO" && !column.Default.Valid {
				fieldNames = append(fieldNames, column.Field)
				placeholders = append(placeholders, "?")
				values = append(values, "")
			}
		}
	}

	//for fieldName, value := range params {
	//	_, exists := Find(colNames, fieldName)
	//	if fieldName != pk_name && exists {
	//		fieldNames = append(fieldNames, fieldName)
	//		placeholders = append(placeholders, "?")
	//		values = append(values, value)
	//	}
	//}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(fieldNames, ", "), strings.Join(placeholders, ", "))

	//result, err = dbExplorer.DB.Exec(fmt.Sprintf("INSERT INTO %s (title, description) VALUES (?, ?)", tableName), "db_crud", "")
	result, err = dbExplorer.DB.Exec(stmt, values...)
	if err != nil {
		return "", err
	}

	affected, err = result.RowsAffected()
	if err != nil {
		return "", err
	}
	if affected == 0 {
		return "", fmt.Errorf("No row was inserted!")
	}

	id, err = result.LastInsertId()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("{\"response\": {\"%s\" : %v}}", pk_name, id), nil
	//return &InsertResult{
	//	Id: id,
	//}, nil
}

func (dbExplorer *MyApi) Update(w http.ResponseWriter, r *http.Request) {

}

func (dbExplorer *MyApi) Delete(w http.ResponseWriter, r *http.Request) {
	var err error
	tableName := path.Base(r.URL.Path)

	//попробуем достать id
	var idIntVal int
	id := tableName

	idIntVal, err = strconv.Atoi(id)
	if err == nil {
		tableName = strings.Split(r.URL.Path, "/")[1]
	}

	res, err := dbExplorer.DeleteBydId(tableName, idIntVal)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	makeOutput(w, ApiResponse{
		Response: &res,
	}, http.StatusOK)
}

func (dbExplorer *MyApi) DeleteBydId(tableName string, id int) (interface{}, error) {
	var err error
	var result sql.Result
	var affected int64

	pk_name, err := dbExplorer.GetPrimaryKeyColumnName(tableName)
	if err != nil {
		return nil, err
	}

	result, err = dbExplorer.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", tableName, pk_name), id)
	if err != nil {
		return nil, err
	}

	affected, err = result.RowsAffected()
	if err != nil {
		return nil, err
	}
	return &DeleteResult{
		Count: affected,
	}, nil
}

func (dbExplorer *MyApi) GetPrimaryKeyColumnName(tableName string) (string, error) {
	var pk_name string
	err := dbExplorer.DB.QueryRow(fmt.Sprintf("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_NAME = '%s'  AND CONSTRAINT_NAME = 'PRIMARY'", tableName)).Scan(&pk_name)
	if err != nil {
		return "", err
	}
	return pk_name, nil
}
