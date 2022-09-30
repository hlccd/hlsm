package ssTable

type Table struct {
	index int
	table *SSTable
	next  *Table
}

func NewTable(index int, table *SSTable) *Table {
	return &Table{
		index: index,
		table: table,
		next:  nil,
	}
}
