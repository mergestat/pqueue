package introspect

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v4/pgxpool"
)

// TableColumn holds information about a column in a table
type TableColumn struct {
	Name     string
	Type     string
	Nullable bool
	Default  sql.NullString
}

func LookupTableColumns(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]*TableColumn, error) {
	rows, err := pool.Query(ctx, "SELECT column_name, column_default, is_nullable, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2", schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := []*TableColumn{}

	for rows.Next() {
		var columnName, nullable, dataType string
		var columnDefault sql.NullString
		if err := rows.Scan(&columnName, &columnDefault, &nullable, &dataType); err != nil {
			return nil, err
		}
		c := &TableColumn{
			Name:    columnName,
			Type:    dataType,
			Default: columnDefault,
		}
		if nullable == "YES" {
			c.Nullable = true
		}
		cols = append(cols, c)
	}

	return cols, nil
}
