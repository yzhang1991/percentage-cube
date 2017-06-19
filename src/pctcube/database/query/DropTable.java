package pctcube.database.query;

import pctcube.database.Table;
import pctcube.database.Table.TableVisitor;

public final class DropTable extends QuerySet implements TableVisitor {

    @Override
    public void visit(Table table) {
        StringBuilder builder = new StringBuilder("DROP TABLE ");
        if (m_addIfExists) {
            builder.append("IF EXISTS ");
        }
        builder.append(table.getTableName());
        builder.append(";");
        addQuery(builder.toString());
    }

    public void setAddIfExistsClause(boolean value) {
        m_addIfExists = value;
    }

    private boolean m_addIfExists = false;
}
