package pctcube.sql;

import pctcube.Errors;
import pctcube.database.Column;
import pctcube.database.Column.ColumnVisitor;
import pctcube.database.Table;
import pctcube.database.Table.TableVisitor;

public final class DropStatementGenerator implements TableVisitor, ColumnVisitor {

    public DropStatementGenerator(StringBuilder builder) {
        m_builder = builder;
    }

    public DropStatementGenerator(StringBuilder builder, boolean addIfExists) {
        m_builder = builder;
        m_addIfExists = addIfExists;
    }

    @Override
    public void visit(Column column) {
        if (column.getTableThisBelongsTo() == null) {
            Errors.PARENT_TABLE_NOT_FOUND.throwIt(null, column.getColumnName());
        }
        m_builder.append("ALTER TABLE ").append(column.getTableThisBelongsTo().getTableName());
        m_builder.append(" DROP COLUMN ").append(column.getColumnName()).append(";\n");
    }

    @Override
    public void visit(Table table) {
        m_builder.append("DROP TABLE ");
        if (m_addIfExists) {
            m_builder.append("IF EXISTS ");
        }
        m_builder.append(table.getTableName());
        m_builder.append(";\n");
    }

    private StringBuilder m_builder;
    private boolean m_addIfExists = false;
}
