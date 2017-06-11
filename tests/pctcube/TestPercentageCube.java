package pctcube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Database;
import pctcube.database.Table;
import pctcube.sql.CreateStatementGenerator;

public class TestPercentageCube {

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
                String.format(Errors.ARGUMENT_NOT_FOUND.getMessage(), "table"));
        verifyCubeInstantiationFails("table=;dimensions=col1,col2,col3; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "table"));
        verifyCubeInstantiationFails("table=nonexist ;dimensions=col1,col2,col3; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "table"));
        // dimensions
        verifyCubeInstantiationFails("table=T ;;measure=measure;",
                String.format(Errors.ARGUMENT_NOT_FOUND.getMessage(), "dimensions"));
        verifyCubeInstantiationFails("table=T ;dimensions=; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "dimensions"));
        verifyCubeInstantiationFails("table=T ;dimensions=nonexist; measure=measure;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "dimensions"));
        // measure
        verifyCubeInstantiationFails("table=T ;dimensions=col1,col2,col3;;;",
                String.format(Errors.ARGUMENT_NOT_FOUND.getMessage(), "measure"));
        verifyCubeInstantiationFails("table=T ;dimensions=col1,col2,col3; measure=;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "measure"));
        verifyCubeInstantiationFails("table=T ;dimensions=col1,col2,col3; measure=abc;",
                String.format(Errors.INVALID_PERCENTAGE_CUBE_ARGS.getMessage(), "measure"));
    }

    @Test
    public void testPercentageCubeTableSchema() {
        PercentageCube cube = new PercentageCube(m_database, new String[]{"table=T ;dimensions=col1,col2,col3; measure=measure;"});
        Table expectedPctCubeTable = new Table("pct");
        expectedPctCubeTable.addColumn(new Column("total by", DataType.VARCHAR).setNullable(false));
        expectedPctCubeTable.addColumn(new Column("break down by", DataType.VARCHAR).setNullable(false));
        expectedPctCubeTable.addColumn(m_col1);
        expectedPctCubeTable.addColumn(m_col2);
        expectedPctCubeTable.addColumn(m_col3);
        expectedPctCubeTable.addColumn(new Column("measure%", DataType.FLOAT).setNullable(false));
        assertEquals(expectedPctCubeTable.toString(), cube.toString());
    }

    @Test
    public void testPercentageCube() {
        PercentageCube cube = new PercentageCube(m_database, new String[]{"table=T ;dimensions=col1,col2,col3; measure=measure;"});
        StringBuilder builder = new StringBuilder();
        CreateStatementGenerator createStatementGen = new CreateStatementGenerator(builder);
        createStatementGen.setAddDropIfExists(true);
        cube.accept(createStatementGen);
        System.out.println(builder.toString());
    }

    private static final Database m_database = new Database();
    private static final Table m_table = new Table("T");
    private static final Column m_col1 = new Column("col1", DataType.INTEGER);
    private static final Column m_col2 = new Column("col2", DataType.VARCHAR);
    private static final Column m_col3 = new Column("col3", DataType.VARCHAR);
    private static final Column m_measure = new Column("measure", DataType.FLOAT);
    static {
        m_table.addColumn(m_col1);
        m_table.addColumn(m_col2);
        m_table.addColumn(m_col3);
        m_table.addColumn(m_measure);
        m_database.addTable(m_table);
    }

}
