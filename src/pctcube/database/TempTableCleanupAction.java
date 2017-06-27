package pctcube.database;

import java.util.logging.Logger;

import pctcube.database.Database.DatabaseVisitor;
import pctcube.database.query.DropTableQuerySet;
import pctcube.database.query.QuerySet;

public final class TempTableCleanupAction extends QuerySet implements DatabaseVisitor {

    @Override
    public void visit(Database database) {
        clear();
        DropTableQuerySet dropTbl = new DropTableQuerySet();
        for (Table table : database.getTables()) {
            if (table.isTempTable()) {
                database.dropTable(table);
                table.accept(dropTbl);
//                m_logger.log(Level.INFO, m_traceMessage, table.getTableName());
            }
        }
        addAllQueries(dropTbl.getQueries());
    }

    private static final String m_traceMessage = "Dropping temp table {0}.";
    private static final Logger m_logger = Logger.getLogger(TempTableCleanupAction.class.getName());
}
