package pctcube;

import pctcube.database.Table;
import pctcube.database.query.QuerySet;

public class TotalLevelAggregation extends QuerySet {

    public TotalLevelAggregation(PercentageAggregation pctAgg) {
        StringBuilder builder = new StringBuilder();
        // Create temp table
        Table tempTable = new Table(TABLE_NAME_PREFIX + pctAgg.getDatabase().getNextTempTableSeqId());
        tempTable.setTempTable(true);
    }

    private static final String TABLE_NAME_PREFIX = "Ttotal_";
}
