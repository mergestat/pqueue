package ddl

import (
	"fmt"
	"strings"

	"github.com/lib/pq"
)

type tableBuilder struct {
	schema      string
	name        string
	ifNotExists bool
	columns     []*columnDef
}

type columnDef struct {
	name     string
	dataType string
	extras   []string
}

// NewTableBuilder returns a builder for producing CREATE TABLE statements
func NewTableBuilder(tableName string) *tableBuilder {
	return &tableBuilder{
		schema:      "public",
		name:        tableName,
		ifNotExists: false,
		columns:     make([]*columnDef, 0),
	}
}

// SetIfNotExists sets if the CREATE TABLE statement should include IF NOT EXISTS or not
func (b *tableBuilder) SetIfNotExists(ifNotExists bool) *tableBuilder {
	b.ifNotExists = ifNotExists
	return b
}

// SetSchema sets the schema of the current builder. By default, "public" is used
func (b *tableBuilder) SetSchema(schema string) *tableBuilder {
	b.schema = schema
	return b
}

// AddColumn adds a column definition to the table. `extras` are joined by a space when SQL is generated
func (b *tableBuilder) AddColumn(name string, dataType string, extras ...string) *tableBuilder {
	b.columns = append(b.columns, &columnDef{name: name, dataType: dataType, extras: extras})
	return b
}

// SQL produces the SQL string for creating a table
func (b *tableBuilder) SQL() string {
	var sql strings.Builder

	sql.WriteString("CREATE TABLE ")

	if b.ifNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}

	if b.schema != "" {
		sql.WriteString(pq.QuoteIdentifier(b.schema) + ".")
	}

	sql.WriteString(pq.QuoteIdentifier(b.name) + " ")

	cols := make([]string, len(b.columns))
	for c, col := range b.columns {
		cols[c] = fmt.Sprintf("%s %s %s", pq.QuoteIdentifier(col.name), col.dataType, strings.Join(col.extras, " "))
	}

	sql.WriteString("(" + strings.Join(cols, ", ") + ")")

	return sql.String()
}
