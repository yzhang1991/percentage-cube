package pctcube.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import pctcube.Errors;
import pctcube.database.query.CreateTable;

public class TestTable {

    @Test
    public void testTable() {
        String dropIfExists = "DROP TABLE IF EXISTS testTable;\n";
        String expectedDDL = "CREATE TABLE testTable (\n" +
                             "    col0 INTEGER,\n" +
                             "    str1 VARCHAR(80),\n" +
                             "    str2 VARCHAR(15),\n" +
                             "    col3 DATE,\n" +
                             "    num DECIMAL(16,5)\n);";
        Table table = new Table("testTable");
        table.addColumn(new Column("col0", DataType.INTEGER));
        table.addColumn(new Column("str1", DataType.VARCHAR));
        table.addColumn(new Column("str2", DataType.VARCHAR, 15));
        table.addColumn(new Column("col3", DataType.DATE));
        table.addColumn(new Column("num", DataType.DECIMAL, 16, 5));

        CreateTable visitor = new CreateTable();
        table.accept(visitor);
        assertEquals(expectedDDL, visitor.toString());

        visitor.clear();
        visitor.setAddDropIfExists(true);
        table.accept(visitor);
        assertEquals(dropIfExists + expectedDDL, visitor.toString());

        try {
            table.addColumn(new Column("col3", DataType.DATE));
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.COLUMN_ALREADY_EXISTS.getMessage(), "col3")));
        }
    }
}
