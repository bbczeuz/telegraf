package sqlquery

import (
	"database/sql"
	_ "errors"
	_ "fmt"
	_ "github.com/mattn/go-oci8"
	"log"
	"strconv"
	//	_ "github.com/go-sql-driver/mysql"
	//	_ "github.com/lib/pq"
	//	_ "github.com/denisenkom/go-mssqldb"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type SqlQuery struct {
	Driver      string
	ServerUrl   string
	Queries     []string
	TagCols     []string
	IntFields   []string
	FloatFields []string
	BoolFields  []string
	ZeroizeNull bool
	//DB *sql.DB //TODO: Avoid reconnects: Push DB driver to struct?
}

var sampleConfig = `
  ## DB Driver
  driver = "oci8" # required. Options: oci8 (Oracle), postgres

  ## Server URL
  server_url = "user/pass@localhost:port/sid" # required

  ## Queries to perform
  queries  = ["SELECT * FROM tablename"] # required
  tag_cols = ["location"] # use these columns as tag keys (cells -> tag values)
  int_fields = ["used_count"] # convert these columns to int64
  float_fields = ["bandwidth_recv"] # convert these columns to float64
  bool_fields = ["is_active"] # convert these columns to bool
  zeroize_null = false # true: Push null results as zeros/empty strings (false: ignore fields)
`

func (s *SqlQuery) SampleConfig() string {
	return sampleConfig
}

func (_ *SqlQuery) Description() string {
	return "Perform SQL query and read results"
}

func (s *SqlQuery) setDefaultValues() {
	if len(s.Driver) == 0 {
		s.Driver = "oci8"
	}

	if len(s.ServerUrl) == 0 {
		s.ServerUrl = "user/passw@localhost:port/sid"
	}

	if len(s.Queries) == 0 {
		s.Queries = []string{"SELECT count(*) FROM tablename"}
	}
}

func contains_str(key string, str_array []string) bool {
	for _, b := range str_array {
		if b == key {
			return true
		}
	}
	return false
}

func (s *SqlQuery) Gather(acc telegraf.Accumulator) error {
	var err error
	drv, dsn := s.Driver, s.ServerUrl

	log.Printf("Input  [sqlquery] Setting up DB...")

	db, err := sql.Open(drv, dsn)
	if err != nil {
		return err
	}
	log.Printf("Input  [sqlquery] Connecting to DB...")
	err = db.Ping()
	if err != nil {
		return err
	}
	defer db.Close()

	//Perform queries
	for _, query := range s.Queries {
		log.Printf("Input  [sqlquery] Performing query '%s'...", query)
		rows, err := db.Query(query)
		if err != nil {
			return err
		}

		defer rows.Close()

		var cols []string
		cols, err = rows.Columns()
		if err != nil {
			return err
		}

		//Split tag and field cols
		col_count := len(cols)
		tag_idx := make([]int, col_count)         //Column indexes of tags
		int_field_idx := make([]int, col_count)   //Column indexes of int fields
		float_field_idx := make([]int, col_count) //Column indexes of int fields
		bool_field_idx := make([]int, col_count)  //Column indexes of int fields
		str_field_idx := make([]int, col_count)   //Column indexes of string fields

		tag_count := 0
		int_field_count := 0
		float_field_count := 0
		bool_field_count := 0
		str_field_count := 0
		for i := 0; i < col_count; i++ {
			if contains_str(cols[i], s.TagCols) {
				tag_idx[tag_count] = i
				tag_count++
			} else if contains_str(cols[i], s.IntFields) {
				int_field_idx[int_field_count] = i
				int_field_count++
			} else if contains_str(cols[i], s.FloatFields) {
				float_field_idx[float_field_count] = i
				float_field_count++
			} else if contains_str(cols[i], s.BoolFields) {
				bool_field_idx[bool_field_count] = i
				bool_field_count++
			} else {
				str_field_idx[str_field_count] = i
				str_field_count++
			}
		}

		log.Printf("Input  [sqlquery] Query '%s' received %d tags and %d (int) + %d (float) + %d (bool) + %d (str) fields...", query, tag_count, int_field_count, float_field_count, bool_field_count, str_field_count)

		//Allocate arrays for field storage
		cells := make([]sql.RawBytes, col_count)
		cell_refs := make([]interface{}, col_count)
		for i := range cells {
			cell_refs[i] = &cells[i]
		}

		row_count := 0

		//Perform splitting
		for rows.Next() {
			//Clear cells (if cells are null, the value is not being updated; leaking of prev values)
			tags := map[string]string{}
			fields := map[string]interface{}{}

			//Parse row
			err := rows.Scan(cell_refs...)
			if err != nil {
				return err
			}

			//Split into tags and fields
			for i := 0; i < tag_count; i++ {
				if cells[tag_idx[i]] != nil {
					//Tags are always strings
					tags[cols[tag_idx[i]]] = string(cells[tag_idx[i]])
				}
			}

			//Extract int fields
			for i := 0; i < int_field_count; i++ {
				if cells[int_field_idx[i]] != nil {
					fields[cols[int_field_idx[i]]], err = strconv.ParseInt(string(cells[int_field_idx[i]]), 10, 64)
					if err != nil {
						return err
					}
				} else if s.ZeroizeNull {
					fields[cols[int_field_idx[i]]] = 0
				}
			}
			//Extract float fields
			for i := 0; i < float_field_count; i++ {
				if cells[float_field_idx[i]] != nil {
					fields[cols[float_field_idx[i]]], err = strconv.ParseFloat(string(cells[float_field_idx[i]]), 64)
					if err != nil {
						return err
					}
				} else if s.ZeroizeNull {
					fields[cols[float_field_idx[i]]] = 0.0
				}
			}
			//Extract bool fields
			for i := 0; i < bool_field_count; i++ {
				if cells[bool_field_idx[i]] != nil {
					fields[cols[bool_field_idx[i]]], err = strconv.ParseBool(string(cells[bool_field_idx[i]]))
					if err != nil {
						return err
					}
				} else if s.ZeroizeNull {
					fields[cols[bool_field_idx[i]]] = false
				}
			}
			//Extract remaining fields as strings
			for i := 0; i < str_field_count; i++ {
				if cells[str_field_idx[i]] != nil {
					fields[cols[str_field_idx[i]]] = string(cells[str_field_idx[i]])
				} else if s.ZeroizeNull {
					fields[cols[str_field_idx[i]]] = ""
				}
			}
			acc.AddFields("sqlquery", fields, tags)
			row_count += 1
		}
		log.Printf("Input  [sqlquery] Query '%s' pushed %d rows...", query, row_count)
	}

	return nil
}

func init() {
	inputs.Add("sqlquery", func() telegraf.Input {
		return &SqlQuery{}
	})
}
