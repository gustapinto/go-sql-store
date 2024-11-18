package ddl

type ColumnDataType string

type Constraint struct {
	Type  string
	Name  string
	Value string
}

type Column struct {
	Name        string
	DataType    ColumnDataType
	Constraints []Constraint
}

const (
	ColumnDataTypeText      ColumnDataType = "TEXT"
	ColumnDataTypeFloat     ColumnDataType = "FLOAT"
	ColumnDataTypeInteger   ColumnDataType = "INTEGER"
	ColumnDataTypeTimestamp ColumnDataType = "TIMESTAMP"

	ConstraintPrimaryKey = "PRIMARY_KEY"
	ConstranintUnique    = "UNIQUE"
)
