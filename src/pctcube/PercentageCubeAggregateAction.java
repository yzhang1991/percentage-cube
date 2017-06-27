package pctcube;

import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;
import pctcube.database.query.QuerySet;
import pctcube.utils.CombinationGenerator;

public class PercentageCubeAggregateAction implements PercentageCubeVisitor {
    // Evaluate all the aggregations that can be re-used during cube evaluation.
    @Override
    public void visit(PercentageCube cube) {
        StringBuilder aggregationQueryBuilder = new StringBuilder();
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);

        List<Column> dimensions = cube.getDimensions();
        CombinationGenerator<Column> dimensionSelector = new CombinationGenerator<>(dimensions);
        // <1>, select the number of GROUP-BY columns.
        // This should be in descending order because aggregate queries with fewer GROUP-BY columns can be
        // derived from the results of aggregate queries with more GROUP-BY's.
        for (int numOfSelectedDimensions = dimensions.size();
                numOfSelectedDimensions >= 1;
                numOfSelectedDimensions--) {
            // <2>, exhaust all the possible selections of [numOfSelectedDimensions] GROUP-BY columns from
            // all the dimensions. This is done through a CombinationGenerator.
            dimensionSelector.setNumOfElementsToSelect(numOfSelectedDimensions);
            for (List<Column> selectedDimensions : dimensionSelector) {
                // Here we generate the aggregate query for each combination of GROUP-BY columns.
                // The query result will be stored in a temporary table generated below.
                // The TempTableCleanupAction class will generate the queries to delete them in the final phase.

                // AggregationTempTable is a sub-class of Table, it can generate the name using the selected
                // dimension column indices so that later cube generation queries can easily find the use them.
                AggregationTempTable tempAggTable = new AggregationTempTable(dimensionSelector.getCurrentSelectionFlags());
                for (Column dimension : selectedDimensions) {
                    // Note that we make a copy of the dimension column object using the deep copy constructor.
                    // Because the columns are associated with the table they are added to, we do not want to mess with
                    // that information here.
                    tempAggTable.addColumn(new Column(dimension));
                }
                tempAggTable.addColumn(new Column("cnt", DataType.INTEGER));
                // The SUM column below, using the same column name as the measure.
                tempAggTable.addColumn(new Column(cube.getMeasure()));
                // Generate CREATE TABLE DDL.
                tempAggTable.accept(createTableQuerySet);

                // The query we are generating below is:
                // INSERT INTO [aggregate table]
                //     SELECT [dimensions], COUNT(*), SUM([measure])
                //     FROM [fact table] | [another aggregate this can derive from]
                //     GROUP BY [dimensions];
                aggregationQueryBuilder.setLength(0);
                aggregationQueryBuilder.append("INSERT INTO ");
                aggregationQueryBuilder.append(tempAggTable.getTableName());
                aggregationQueryBuilder.append("\n").append(QuerySet.getIndentationString(1));
                aggregationQueryBuilder.append("SELECT ");
                for (Column dimension : selectedDimensions) {
                    aggregationQueryBuilder.append(dimension.getQuotedColumnName()).append(", ");
                }
                // Force the measure column name to be identical across the tables.
                aggregationQueryBuilder.append("COUNT(*), SUM(").append(cube.getMeasure().getQuotedColumnName());
                aggregationQueryBuilder.append(")\n").append(QuerySet.getIndentationString(1));
                aggregationQueryBuilder.append("FROM ");
                // Reuse the aggregation result if possible.
                if (numOfSelectedDimensions < dimensions.size()) {
                    // Not all the dimension columns are used as GROUP-BY columns, we can reuse
                    // the aggregate results from a more granular level to save time.
                    List<Table> tables = cube.getDatabase().getTables();
                    // Search from the back (more recently created tables) to get the most optimal result.
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
                    // If this is the aggregation at the finest level, i.e.,
                    // it is using all the dimension columns as GROUP-BY columns,
                    // it can only be computed from the fact table.
                    aggregationQueryBuilder.append(cube.getFactTable().getTableName());
                }
                aggregationQueryBuilder.append("\n").append(QuerySet.getIndentationString(1));
                aggregationQueryBuilder.append("GROUP BY ");
                for (Column col : selectedDimensions) {
                    aggregationQueryBuilder.append(col.getQuotedColumnName()).append(", ");
                }
                aggregationQueryBuilder.setLength(aggregationQueryBuilder.length() - 2);
                aggregationQueryBuilder.append(";");
                cube.getDatabase().addTable(tempAggTable);
                cube.addAllQueries(createTableQuerySet.getQueries());
                cube.addQuery(aggregationQueryBuilder.toString());
                createTableQuerySet.clear();
            }
        }

        // Get the global count, used in cases when total-by key set is empty.
        // The query is:
        // INSERT INTO TEMP_AGG_COUNT
        // SELECT COUNT(*), SUM([measure]) FROM TEMP_AGG_0;
        // TEMP_AGG_0 is one of the closest aggregation results this global count query can reuse.

        // Create the table:
        Table globalAggTable = new Table(TEMP_AGG_COUNT);
        globalAggTable.setTempTable(true);
        globalAggTable.addColumn(new Column("cnt", DataType.INTEGER));
        globalAggTable.addColumn(new Column(cube.getMeasure()));
        globalAggTable.accept(createTableQuerySet);

        // Build the query:
        aggregationQueryBuilder.setLength(0);
        aggregationQueryBuilder.append("INSERT INTO ");
        aggregationQueryBuilder.append(globalAggTable.getTableName());
        aggregationQueryBuilder.append("\n").append(QuerySet.getIndentationString(1));
        aggregationQueryBuilder.append("SELECT COUNT(*), ");
        aggregationQueryBuilder.append("SUM(").append(cube.getMeasure().getQuotedColumnName());
        aggregationQueryBuilder.append(")\n").append(QuerySet.getIndentationString(1));
        aggregationQueryBuilder.append("FROM TEMP_AGG_0;");

        cube.getDatabase().addTable(globalAggTable);
        cube.addAllQueries(createTableQuerySet.getQueries());
        cube.addQuery(aggregationQueryBuilder.toString());
    }

    protected static final String TEMP_AGG_COUNT = "TEMP_AGG_COUNT";
}
