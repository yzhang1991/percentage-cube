package pctcube.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import pctcube.Errors;
import pctcube.sql.CreateStatementGenerator;
import pctcube.sql.DropStatementGenerator;

public class TestTable {

    @Test
    public void testTable() {
        String dropIfExists = "DROP TABLE IF EXISTS testTable;\n";
        String expectedDDL = "CREATE TABLE testTable (\n" +
                             "    col0 INTEGER,\n" +
                             "    str1 VARCHAR(80),\n" +
                             "    str2 VARCHAR(15),\n" +
                             "    col3 DATE,\n" +
                             "    num DECIMAL(16,5)\n);\n";
        Table table = new Table("testTable");
        table.addColumn(new Column("col0", DataType.INTEGER));
        table.addColumn(new Column("str1", DataType.VARCHAR));
        table.addColumn(new Column("str2", DataType.VARCHAR, 15));
        table.addColumn(new Column("col3", DataType.DATE));
        table.addColumn(new Column("num", DataType.DECIMAL, 16, 5));

        StringBuilder builder = new StringBuilder();
        CreateStatementGenerator visitor = new CreateStatementGenerator(builder);
        table.accept(visitor);
        assertEquals(expectedDDL, builder.toString());

        builder.setLength(0);
        visitor.setAddDropIfExists(true);
        table.accept(visitor);
        assertEquals(dropIfExists + expectedDDL, builder.toString());

        try {
            table.addColumn(new Column("col3", DataType.DATE));
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.COLUMN_ALREADY_EXISTS.getMessage(), "col3")));
        }

        // Test "ALTER TABLE DROP COLUMN" statement generation
        builder.setLength(0);
        DropStatementGenerator dropColumnTester = new DropStatementGenerator(builder);
        table.getColumnByName("num").accept(dropColumnTester);
        assertEquals("ALTER TABLE testTable DROP COLUMN num;\n", builder.toString());

        Column isolatedColumn = new Column("num", DataType.DECIMAL, 16, 5);
        builder.setLength(0);
        try {
            isolatedColumn.accept(dropColumnTester);
        }
        catch (RuntimeException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.PARENT_TABLE_NOT_FOUND.getMessage(), "num")));
        }
    }
}
