package parser

const (
	TypeInsertOperation = "INSERT"
	TypeDatabase        = "DATABASE"
	TypeTableDefinition = "TABLE_DEFINITION"
	TypeTable           = "TABLE"
	TypeColumnList      = "COLUMN_LIST"
	TypeColumn          = "COLUMN"
	TypeValueList       = "VALUE_LIST"
	TypeValue           = "VALUE"
)

type AST struct {
	Type     string
	Value    string
	Parent   *AST
	Children []*AST
}

//
//var root = &AST{
//	Type: TypeInsertOperation,
//	Children: []*AST{
//		{
//			Type: TypeTableDefinition,
//			Children: []*AST{
//				{
//					Type:  TypeDatabase,
//					Value: "database",
//				},
//				{
//					Type:  TypeTable,
//					Value: "table",
//				},
//				{
//					Type: TypeColumnList,
//					Children: []*AST{
//						{
//							Type:  TypeColumn,
//							Value: "id",
//						},
//						{
//							Type:  TypeColumn,
//							Value: "value",
//						},
//					},
//				},
//				{
//					Type: TypeValueList,
//					Children: []*AST{
//						{
//							Type:  TypeValue,
//							Value: "1",
//						},
//						{
//							Type:  TypeValue,
//							Value: "'test'",
//						},
//					},
//				},
//			},
//		},
//	},
//}
