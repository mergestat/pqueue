package ddl_test

import (
	"testing"

	"github.com/mergestat/pgq/ddl"
	pg_query "github.com/pganalyze/pg_query_go"
)

func TestTableBuilderParseOK(t *testing.T) {
	stms := []string{
		ddl.NewTableBuilder("simple_table").
			AddColumn("col1", "text").
			AddColumn("col2", "int").
			SQL(),
		ddl.NewTableBuilder("simple_table").
			SetIfNotExists(true).
			AddColumn("id", "int", "PRIMARY KEY").
			AddColumn("col2", "int", "PRIMARY KEY").
			SQL(),
		ddl.NewTableBuilder("simple_table").
			SetSchema("some_schema").
			AddColumn("id", "int", "PRIMARY KEY").
			AddColumn("col2", "int", "PRIMARY KEY").
			SQL(),
	}

	for _, sql := range stms {
		_, err := pg_query.ParseToJSON(sql)
		if err != nil {
			t.Fatal(err)
		}
	}
}
