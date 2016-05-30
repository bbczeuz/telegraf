package system

import (
	_ "errors"
	_ "fmt"
	"database/sql"
	_ "github.com/mattn/go-oci8"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type SqlQuery struct {
	Name        string
	Driver      string
	ServerUrl   string
	Queries   []string
	TagCols   []string
	DB *sql.DB
}

var sampleConfig = `
  ## Measurement name
  name = "sqlquery" # required

  ## DB Driver
  driver = "oci8" # required. Options: oci8 (Oracle), postgres

  ## Server URL
  server_url = "user/pass@localhost:port/sid" # required

  ## Queries to perform
  queries  = ["SELECT * FROM tablename"] # required
  tag_cols = ["location"] # use these columns as tag keys (cells -> tag values)
`

func (s *SqlQuery) SampleConfig() string {
        return sampleConfig
}

func (_ *SqlQuery) Description() string {
	return "Perform SQL query and read results"
}

func (s *SqlQuery) setDefaultValues() {
	if len(s.Name) == 0 {
		s.Name = "sqlquery"
	}

	if len(s.Driver) == 0 {
		s.Name = "oci8"
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
	db, err := sql.Open(drv, dsn)
	if err != nil {
		return err
//		log.Fatalf("cannot connect with %q to %q: %v", drv, dsn, err)
	}
	err = db.Ping()
	if err != nil {
		return err
//		log.Fatalf("cannot connect with %q to %q: %v", drv, dsn, err)
	}
	defer db.Close()

	//Perform queries
	for _, query := range s.Queries {
		rows, err := db.Query(query);
		if err != nil {
			return err;
		}

		defer rows.Close()
		
		var cols []string;
		cols, err = rows.Columns()
		if err != nil {
			return err;
		}

		//Split tag and field cols
		col_count   := len(cols)
		tag_idx     := make([]int, col_count); //Column indexes of tags
		field_idx   := make([]int, col_count); //Column indexes of fields

		tag_count   := 0;
		field_count := 0;
		for i := 0; i<col_count; i++ {
			if contains_str(cols[i], s.TagCols) {
				tag_idx[tag_count] = i
				tag_count++
			} else {
				field_idx[field_count] = i
				field_count++
			}
		}


		cells  := make([]sql.RawBytes, col_count);
		cell_refs := make([]interface{}, col_count);
		fields := map[string]interface{}{};
		tags   := map[string]string{};
		for i:= range cells {
			cell_refs[i] = &cells[i];
		}

		for rows.Next() {
			err := rows.Scan(cell_refs...);
			if err != nil {
				return err;
			}

			//Split into tags and fields
			for i:= 0; i<tag_count; i++ {
				if (cells[tag_idx[i]] != nil) {
					tags[cols[tag_idx[i]]] = string(cells[tag_idx[i]]);
				}
			}
			for i:= 0; i<field_count; i++ {
				if (cells[field_idx[i]] != nil) {
					fields[cols[field_idx[i]]] = string(cells[field_idx[i]]);
				}
			}
			//return errors.New(fmt.Sprintf("Field[%s] = %s\n", cols[field_idx[0]], fields[cols[field_idx[0]]]))
/*
			fields := map[string]interface{}{
				"total":      123,
				"available":  456,
				"used":       789,
			}
			tags   := map[string]string{
				"tagkey0":  "tagval0",
				"tagkey1":  "tagval1",
				"tagkey2":  "tagval2",
			}
*/
			acc.AddFields(s.Name, fields, tags)
		}

	}

	return nil
}

func init() {
	inputs.Add("sqlquery", func() telegraf.Input {
		return &SqlQuery{}
	})
}
