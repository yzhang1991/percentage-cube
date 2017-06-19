package pctcube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Database;
import pctcube.database.Table;
import pctcube.database.query.CreateTable;

public class TestPercentageCube {

    @Test
    public void testPercentageCube() {
        PercentageCube cube = new PercentageCube(m_database,
                new String[] {"table=T ;dimensions=col1,col2,col3; measure=measure;"});
        System.out.println(cube.toString());
    }

    private void verifyCubeInstantiationFails(String argument, String expectedErrorMessage) {
        try {
            @SuppressWarnings("unused")
            PercentageCube cube = new PercentageCube(m_database, new String[]{argument});
            fail("Expected an exception, but nothing happened.");
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(expectedErrorMessage));
        }
    }

    @Test
    public void testInvalidCubeParameters() {
        // table
        verifyCubeInstantiationFails("dimensions=col1,col2,col3; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "table"));
        verifyCubeInstantiationFails("table=;dimensions=col1,col2,col3; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "table"));
        verifyCubeInstantiationFails("table=nonexist ;dimensions=col1,col2,col3; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "table"));
        // dimensions
        verifyCubeInstantiationFails("table=T ;;measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "dimensions"));
        verifyCubeInstantiationFails("table=T ;dimensions=; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "dimensions"));
        verifyCubeInstantiationFails("table=T ;dimensions=nonexist; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "dimensions"));
        // measure
        verifyCubeInstantiationFails("table=T ;dimensions=col1,col2,col3;;;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "measure"));
        verifyCubeInstantiationFails("table=T ;dimensions=col1,col2,col3; measure=;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "measure"));
        verifyCubeInstantiationFails("table=T ;dimensions=col1,col2,col3; measure=abc;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "measure"));

        // need to test more arguments
    }

    @Test
    public void testPercentageCubeTableSchema() {
        PercentageCube cube = new PercentageCube(m_database,
                new String[]{"table=T ;dimensions=col1,col2,col3; measure=measure;"});
        Table expectedPctCubeTable = new Table("pct");
        expectedPctCubeTable.addColumn(new Column("total by", DataType.VARCHAR).setNullable(false));
        expectedPctCubeTable.addColumn(new Column("break down by", DataType.VARCHAR).setNullable(false));
        expectedPctCubeTable.addColumn(m_col1);
        expectedPctCubeTable.addColumn(m_col2);
        expectedPctCubeTable.addColumn(m_col3);
        expectedPctCubeTable.addColumn(new Column("measure%", DataType.FLOAT).setNullable(false));
        assertEquals(expectedPctCubeTable.toString(),
                PercentageCubeTableFactory.getTable(cube).toString());
    }

    @Test
    public void testPercentageCubeTable() {
        PercentageCube cube = new PercentageCube(
                m_database, new String[]{"table=T ;dimensions=col1,col2,col3; measure=measure;"});
        CreateTable createStatementGen = new CreateTable();
        createStatementGen.setAddDropIfExists(true);
        cube.accept(createStatementGen);
        String expectedDDL = "DROP TABLE IF EXISTS pct;\n" +
                             "CREATE TABLE pct (\n" +
                             "    \"total by\" VARCHAR(80) NOT NULL,\n" +
                             "    \"break down by\" VARCHAR(80) NOT NULL,\n" +
                             "    col1 INTEGER,\n" +
                             "    col2 VARCHAR(80),\n" +
                             "    col3 VARCHAR(80),\n" +
                             "    \"measure%\" FLOAT NOT NULL\n" +
                             ");";
        assertEquals(expectedDDL, createStatementGen.toString());
    }

    protected static final Database m_database = new Database();
    protected static final Table m_table = new Table("T");
    protected static final Column m_col1 = new Column("col1", DataType.INTEGER);
    protected static final Column m_col2 = new Column("col2", DataType.VARCHAR);
    protected static final Column m_col3 = new Column("col3", DataType.VARCHAR);
    protected static final Column m_measure = new Column("measure", DataType.FLOAT);
    static {
        m_table.addColumn(m_col1);
        m_table.addColumn(m_col2);
        m_table.addColumn(m_col3);
        m_table.addColumn(m_measure);
        m_database.addTable(m_table);
    }
}
