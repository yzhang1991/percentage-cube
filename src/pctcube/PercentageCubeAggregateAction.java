package pctcube;

import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;
import pctcube.utils.CombinationGenerator;

public class PercentageCubeAggregateAction implements PercentageCubeVisitor {

    // Evaluate all the aggregations
    @Override
    public void visit(PercentageCube cube) {
        StringBuilder aggregationQueryBuilder = new StringBuilder();
        CreateTableQuerySet createTable = new CreateTableQuerySet();
        createTable.setAddDropIfExists(true);

        List<Column> dimensions = cube.getDimensions();
        CombinationGenerator<Column> dimensionSelector = new CombinationGenerator<>(dimensions);
        for (int numOfSelectedDimensions = dimensions.size();
                numOfSelectedDimensions >= 1;
                numOfSelectedDimensions--) {
            dimensionSelector.setNumOfElementsToSelect(numOfSelectedDimensions);
            for (List<Column> combination : dimensionSelector) {
                AggregationTempTable tempAggTable = new AggregationTempTable(dimensionSelector.getCurrentSelectionFlags());
                aggregationQueryBuilder.setLength(0);
                aggregationQueryBuilder.append("INSERT INTO ");
                aggregationQueryBuilder.append(tempAggTable.getTableName());
                aggregationQueryBuilder.append("\n").append(cube.getIndentationString(1));
                aggregationQueryBuilder.append("SELECT ");
                for (Column col : combination) {
                    tempAggTable.addColumn(new Column(col));
                    aggregationQueryBuilder.append(col.getQuotedColumnName()).append(", ");
                }
                tempAggTable.addColumn(new Column("cnt", DataType.INTEGER));
                tempAggTable.addColumn(new Column(cube.getMeasure()));
                createTable.clear();
                // Generate CREATE TABLE DDL.
                tempAggTable.accept(createTable);

                aggregationQueryBuilder.append("COUNT(*), SUM(").append(cube.getMeasure().getQuotedColumnName());
                // Force the measure column name to be identical across the tables.
                aggregationQueryBuilder.append(")\n").append(cube.getIndentationString(1));
                aggregationQueryBuilder.append("FROM ");
                // Reuse the aggregation result if possible.
                if (numOfSelectedDimensions < dimensions.size()) {
                    // can reuse results
                    List<Table> tables = cube.getDatabase().getTables();
                    // search from the back to get optimal result
                    for (int i = tables.size() - 1; i >= 0; i--) {
                        Table existingAggResult = tables.get(i);
                        if (! (existingAggResult instanceof AggregationTempTable)) {
                            continue;
                        }
                        if (tempAggTable.canDeriveFrom((AggregationTempTable)existingAggResult)) {
                            aggregationQueryBuilder.append(existingAggResult.getTableName());
                            break;
                        }
                    }
                }
                else {
                    // aggregation at the finest level, compute from the fact table.
                    aggregationQueryBuilder.append(cube.getFactTable().getTableName());
                }
                aggregationQueryBuilder.append("\n").append(cube.getIndentationString(1));
                aggregationQueryBuilder.append("GROUP BY ");
                for (Column col : combination) {
                    aggregationQueryBuilder.append(col.getQuotedColumnName()).append(", ");
                }
                aggregationQueryBuilder.setLength(aggregationQueryBuilder.length() - 2);
                aggregationQueryBuilder.append(";");
                cube.getDatabase().addTable(tempAggTable);
                cube.addAllQueries(createTable.getQueries());
                cube.addQuery(aggregationQueryBuilder.toString());
            }
        }

        // Get the global count, used in cases when total-by key set is empty.
        Table globalAggTable = new Table(TEMP_AGG_COUNT);
        globalAggTable.setTempTable(true);
        globalAggTable.addColumn(new Column("cnt", DataType.INTEGER));
        globalAggTable.addColumn(new Column(cube.getMeasure()));
        createTable.clear();
        globalAggTable.accept(createTable);
        aggregationQueryBuilder.setLength(0);
        aggregationQueryBuilder.append("INSERT INTO ");
        aggregationQueryBuilder.append(globalAggTable.getTableName());
        aggregationQueryBuilder.append("\n").append(cube.getIndentationString(1));
        aggregationQueryBuilder.append("SELECT COUNT(*), ");
        aggregationQueryBuilder.append("SUM(").append(cube.getMeasure().getQuotedColumnName());
        // Force the measure column name to be identical across the tables.
        aggregationQueryBuilder.append(")\n").append(cube.getIndentationString(1));
        aggregationQueryBuilder.append("FROM TEMP_AGG_0;");
        cube.getDatabase().addTable(globalAggTable);
        cube.addAllQueries(createTable.getQueries());
        cube.addQuery(aggregationQueryBuilder.toString());
    }

    protected static final String TEMP_AGG_COUNT = "TEMP_AGG_COUNT";
}
