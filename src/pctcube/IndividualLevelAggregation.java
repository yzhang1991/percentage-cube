package pctcube;

import pctcube.PercentageAggregation.PercentageAggregationVisitor;
import pctcube.database.Column;
import pctcube.database.Table;
import pctcube.database.query.CreateTable;
import pctcube.database.query.QuerySet;

public class IndividualLevelAggregation extends QuerySet implements PercentageAggregationVisitor {

    protected IndividualLevelAggregation() { }

    @Override
    public void visit(PercentageAggregation agg) {
        m_pctAgg = agg;
        m_pctAgg.m_aggIndv = this;
        createTempTable();
        generateQuery();
    }

    private void generateQuery() {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO ").append(m_tempTableName).append("\n");
        builder.append(getIndentationString(1));
        builder.append("SELECT ");
        for (Column totalByKey : m_pctAgg.getTotalByKeys()) {
            builder.append(totalByKey.getQuotedColumnName());
            builder.append(", ");
        }
        for (Column breakdownByKey : m_pctAgg.getBreakdownByKeys()) {
            builder.append(breakdownByKey.getQuotedColumnName());
            builder.append(", ");
        }
        builder.append("SUM(").append(m_pctAgg.getMeasure().getQuotedColumnName());
        builder.append(") FROM ");
        builder.append(m_pctAgg.getFactTable().getTableName());
        builder.append("\n").append(getIndentationString(1));
        builder.append("GROUP BY ");
        for (Column totalByKey : m_pctAgg.getTotalByKeys()) {
            builder.append(totalByKey.getQuotedColumnName());
            builder.append(", ");
        }
        for (Column breakdownByKey : m_pctAgg.getBreakdownByKeys()) {
            builder.append(breakdownByKey.getQuotedColumnName());
            builder.append(", ");
        }
        builder.setLength(builder.length() - 2);
        builder.append(";");
        m_pctAgg.addQuery(builder.toString());
    }

    private void createTempTable() {
        // Create temp table
        m_tempTableName = TABLE_NAME_PREFIX + m_pctAgg.getDatabase().getNextTempTableSeqId();
        Table tempTable = new Table(m_tempTableName);
        tempTable.setTempTable(true);
        // Add total by keys
        for (Column totalByKey : m_pctAgg.getTotalByKeys()) {
            tempTable.addColumn(new Column(totalByKey));
        }
        // Add break down by keys
        for (Column breakdownByKey : m_pctAgg.getBreakdownByKeys()) {
            tempTable.addColumn(new Column(breakdownByKey));
        }
        // Add SUM(measure)
        tempTable.addColumn(new Column(m_pctAgg.getMeasure()));
        // Add tempTable to the database
        m_pctAgg.getDatabase().addTable(tempTable);
        // Add DDL
        CreateTable createTable = new CreateTable();
        tempTable.accept(createTable);
        m_pctAgg.addAllQueries(createTable.getQueries());
    }

    public String getTempTableName() {
        return m_tempTableName;
    }

    private PercentageAggregation m_pctAgg;
    private String m_tempTableName;
    private static final String TABLE_NAME_PREFIX = "Tindv_";
}
