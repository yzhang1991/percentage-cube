package pctcube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.database.Table;
import pctcube.database.query.QuerySet;

public class PercentageAggregation extends QuerySet {

    protected interface PercentageAggregationVisitor {
        void visit(PercentageAggregation agg);
    }

    public PercentageAggregation(
            Database db, Table factTable, Column measure) {
        m_database = db;
        m_factTable = factTable;
        m_measure = measure;
    }

    public void accept(PercentageAggregationVisitor visitor) {
        visitor.visit(this);
    }

    public void evaluateWithGroupByMethod() {
        // must create the IndividualLevelAggregation first, because
        // the total level may depend on the individual level.
        clear();
        accept(new IndividualLevelAggregation());
        accept(new TotalLevelAggregation());
    }

    public void evaluateWithOLAPMethod() {

    }

    public void addTotalByKey(Column key) {
        m_totalByKeys.add(key);
    }

    public void addBreakdownByKey(Column key) {
        m_breakdownByKeys.add(key);
    }

    public List<Column> getTotalByKeys() {
        return Collections.unmodifiableList(m_totalByKeys);
    }

    public List<Column> getBreakdownByKeys() {
        return Collections.unmodifiableList(m_breakdownByKeys);
    }

    public Column getMeasure() {
        return m_measure;
    }

    public Table getFactTable() {
        return m_factTable;
    }

    protected Database getDatabase() {
        return m_database;
    }

    public boolean reuseResults() {
        return m_reuseResults;
    }

    public void setReuseResults(boolean value) {
        m_reuseResults = value;
    }

    private final List<Column> m_totalByKeys = new ArrayList<>();
    private final List<Column> m_breakdownByKeys = new ArrayList<>();
    private final Column m_measure;
    private final Database m_database;
    private final Table m_factTable;

    private boolean m_reuseResults = false;
    protected IndividualLevelAggregation m_aggIndv;
    protected TotalLevelAggregation m_aggTotal;
    protected FinalJoin m_finalJoin;
}
