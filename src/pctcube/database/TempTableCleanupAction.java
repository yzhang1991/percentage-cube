package pctcube.database;

import java.util.logging.Level;
import java.util.logging.Logger;

import pctcube.database.Database.DatabaseVisitor;

public final class TempTableCleanupAction implements DatabaseVisitor {

    public TempTableCleanupAction() { }

    public TempTableCleanupAction(StringBuilder builder) {
        m_builder = builder;
    }

    @Override
    public void visit(Database database) {
        for (Table t : database.m_tables.values()) {
            if (t.isTempTable()) {
                database.m_tables.remove(t.getTableName());
                m_logger.log(Level.INFO, m_traceMessage, t.getTableName());
                if (m_builder != null) {
                    m_builder.append("");
                }
            }
        }
    }

    private StringBuilder m_builder;

    private static final String m_traceMessage = "Dropped temp table {0}.";
    private static final Logger m_logger = Logger.getLogger(TempTableCleanupAction.class.getName());
}
