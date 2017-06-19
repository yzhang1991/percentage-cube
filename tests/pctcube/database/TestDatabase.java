package pctcube.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import pctcube.Errors;

public class TestDatabase {

    @Test
    public void testDatabase() {
        Database db = new Database();
        Table table1 = new Table("table1");
        table1.addColumn(new Column("col0", DataType.INTEGER));
        table1.addColumn(new Column("str1", DataType.VARCHAR));
        table1.addColumn(new Column("str2", DataType.VARCHAR, 15));
        table1.addColumn(new Column("col3", DataType.DATE).setNullable(false));
        Table table2 = new Table("table1");
        table2.addColumn(new Column("col0", DataType.INTEGER));
        db.addTable(table1);
        try {
            db.addTable(table2);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.TABLE_ALREADY_EXISTS.getMessage(),
                            "table1")));
        }
        table2.setTableName("table2");
        db.addTable(table2);
        String expectedDDL = "CREATE TABLE table1 (\n" +
                             "    col0 INTEGER,\n" +
                             "    str1 VARCHAR(80),\n" +
                             "    str2 VARCHAR(15),\n" +
                             "    col3 DATE NOT NULL\n" +
                             ");\n" +
                             "CREATE TABLE table2 (\n" +
                             "    col0 INTEGER\n" +
                             ");";
        assertEquals(expectedDDL, db.toString());

        TempTableCleanupAction cleaner = new TempTableCleanupAction();
        db.accept(cleaner);
        assertTrue(db.getTableByName("table2") != null);
        table2.setTempTable(true);
        db.accept(cleaner);
        assertTrue(db.getTableByName("table2") == null);
    }

}
