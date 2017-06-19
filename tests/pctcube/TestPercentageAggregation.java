package pctcube;

import org.junit.Test;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Database;
import pctcube.database.Table;

public class TestPercentageAggregation {

    @Test
    public void testPercentageAggregation() {
        PercentageAggregation pa = new PercentageAggregation(m_database, m_table, m_measure);
        pa.addTotalByKey(m_col1);
        pa.addTotalByKey(m_col2);
        pa.addBreakdownByKey(m_col3);
        pa.setReuseResults(true);
//        String expectedDescription = "Percentage aggregation on fact table 'T'\n" +
//                                     "MEASURE: measure\n" +
//                                     "TOTAL BY: col1,col2\n" +
//                                     "BREAK DOWN BY: col3";
//        assertEquals(expectedDescription, pa.toString());



        pa.evaluateWithGroupByMethod();
        System.out.println(pa.toString());
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
