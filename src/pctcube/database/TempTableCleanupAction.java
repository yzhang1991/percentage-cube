package pctcube.database;

import java.util.logging.Level;
import java.util.logging.Logger;

import pctcube.database.Database.DatabaseVisitor;
import pctcube.database.query.DropTable;
import pctcube.database.query.QuerySet;

public final class TempTableCleanupAction extends QuerySet implements DatabaseVisitor {

    @Override
    public void visit(Database database) {
        DropTable dropTbl = new DropTable();
        for (Table t : database.m_tables.values()) {
            if (t.isTempTable()) {
                t.accept(dropTbl);
                database.m_tables.remove(t.getTableName());
                m_logger.log(Level.INFO, m_traceMessage, t.getTableName());
            }
        }
        addAllQueries(dropTbl.getQueries());
    }

    private static final String m_traceMessage = "Dropping temp table {0}.";
    private static final Logger m_logger = Logger.getLogger(TempTableCleanupAction.class.getName());
}
