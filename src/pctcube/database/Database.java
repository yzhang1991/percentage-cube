package pctcube.database;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import pctcube.Errors;
import pctcube.database.query.CreateTable;

public final class Database {

    public interface DatabaseVisitor {
        void visit(Database database);
    }

    public void accept(DatabaseVisitor visitor) {
        visitor.visit(this);
    }

    public void addTables(Table... tables) {
        for (Table table : tables) {
            addTable(table);
        }
    }

    public void addTable(Table table) {
        if (m_tables.containsKey(table.getTableName())) {
            Errors.TABLE_ALREADY_EXISTS.throwIt(null, table.getTableName());
        }
        m_tables.put(table.getTableName(), table);
    }

    public int getNextTempTableSeqId() {
        return m_tempTableSeqId.getAndIncrement();
    }

    public List<Table> getTables() {
        return new ArrayList<Table>(m_tables.values());
    }

    public Table getTableByName(String name) {
        return m_tables.get(name);
    }

    @Override
    public String toString() {
        CreateTable visitor = new CreateTable();
        accept(visitor);
        return visitor.toString();
    }

    // TempTableCleanupAction will access it
    protected Map<String, Table> m_tables = new LinkedHashMap<>();

    private final AtomicInteger m_tempTableSeqId = new AtomicInteger(0);
}
