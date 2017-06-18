package pctcube.sql;

import java.util.Collections;
import java.util.List;

import pctcube.PercentageCube;
import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.PercentageCubeTableFactory;
import pctcube.database.Column;
import pctcube.database.Column.ColumnVisitor;
import pctcube.database.Database;
import pctcube.database.Database.DatabaseVisitor;
import pctcube.database.Table;
import pctcube.database.Table.TableVisitor;

public final class CreateStatementGenerator implements ColumnVisitor, TableVisitor, DatabaseVisitor, PercentageCubeVisitor {

    public CreateStatementGenerator(StringBuilder builder) {
        m_builder = builder;
        setIndentation(4);
    }

    @Override
    public void visit(PercentageCube cube) {
        PercentageCubeTableFactory.getTable(cube).accept(this);
    }

    @Override
    public void visit(Database database) {
        List<Table> tables = database.getTables();
        for (Table table : tables) {
            table.accept(this);
            m_builder.append("\n");
        }
    }

    @Override
    public void visit(Table table) {
        if (m_dropIfExists) {
            DropStatementGenerator visitor = new DropStatementGenerator(m_builder, true);
            table.accept(visitor);
        }
        m_builder.append("CREATE TABLE ").append(table.getTableName()).append(" (\n");
        List<Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            indent();
            c.accept(this);
            if (i != columns.size() - 1) {
                m_builder.append(",");
            }
            m_builder.append("\n");
        }
        m_builder.append(");\n");
    }

    @Override
    public void visit(Column column) {
        m_builder.append(column.getQuotedColumnName()).append(" ");
        m_builder.append(column.getDataType().getTypeName());
        // For variable-length column, append the column size after the column type.
        if (column.getDataType().hasPrecisionAndScale()) {
            m_builder.append("(").append(column.getPrecision());
            m_builder.append(",").append(column.getScale()).append(")");
        }
        else if (column.getDataType().isVariableLengthType()) {
            m_builder.append("(").append(column.getSize()).append(")");
        }
        if (! column.isNullable()) {
            m_builder.append(" NOT NULL");
        }
    }

    public void setIndentation(int size) {
        m_indentation = String.join("", Collections.nCopies(size, " "));
    }

    public void setAddDropIfExists(boolean value) {
        m_dropIfExists = value;
    }

    private void indent() {
        m_builder.append(m_indentation);
    }

    private StringBuilder m_builder;
    private String m_indentation;
    private boolean m_dropIfExists = false;
}
