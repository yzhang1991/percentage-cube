package pctcube.database.query;

import java.util.List;

import pctcube.PercentageCube;
import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.PercentageCubeTableFactory;
import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.database.Database.DatabaseVisitor;
import pctcube.database.Table;
import pctcube.database.Table.TableVisitor;

/**
 * Create the "CREATE" DDL for a table.
 * For a database, create the DDLs for all the tables.
 * For a percentage cube, get the cube table schema from PercentageCubeTableFactory and create the DDL for it.
 * @author yzhang
 */
public final class CreateTableQuerySet extends QuerySet implements TableVisitor, DatabaseVisitor, PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        PercentageCubeTableFactory.getTable(cube).accept(this);
    }

    @Override
    public void visit(Database database) {
        List<Table> tables = database.getTables();
        for (Table table : tables) {
            table.accept(this);
        }
    }

    @Override
    public void visit(Table table) {
        if (m_dropIfExists) {
            DropTableQuerySet dropStmt = new DropTableQuerySet();
            dropStmt.setAddIfExistsClause(true);
            table.accept(dropStmt);
            addAllQueries(dropStmt.getQueries());
        }
        StringBuilder builder = new StringBuilder("CREATE TABLE ");
        builder.append(table.getTableName()).append(" (\n");
        List<Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            builder.append(getIndentationString(1));
            Column c = columns.get(i);
            builder.append(c.toString());
            if (i != columns.size() - 1) {
                builder.append(",");
            }
            builder.append("\n");
        }
        builder.append(");");
        addQuery(builder.toString());
    }

    public void setAddDropIfExists(boolean value) {
        m_dropIfExists = value;
    }

    private boolean m_dropIfExists = false;
}
