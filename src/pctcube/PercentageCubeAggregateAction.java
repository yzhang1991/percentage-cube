package pctcube;

import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.Table;
import pctcube.database.query.CreateTable;
import pctcube.utils.CombinationGenerator;

public class PercentageCubeAggregateAction implements PercentageCubeVisitor {

    // Evaluate all the aggregations
    @Override
    public void visit(PercentageCube cube) {
        StringBuilder aggregationQueryBuilder = new StringBuilder();
        CreateTable createTable = new CreateTable();
        createTable.setAddDropIfExists(true);
        List<Column> dimensions = cube.getDimensions();
        CombinationGenerator<Column> cgen = new CombinationGenerator<>();
        for (Column dimension : dimensions) {
            cgen.addElement(dimension);
        }
        for (int cnt = dimensions.size(); cnt >= 1; cnt--) {
            cgen.setNumOfElementsToSelect(cnt);
            for (List<Column> combination : cgen) {
                AggregationTempTable tempAggTable = new AggregationTempTable(cgen.getCurrentSelectionFlags());
                aggregationQueryBuilder.setLength(0);
                aggregationQueryBuilder.append("INSERT INTO ");
                aggregationQueryBuilder.append(tempAggTable.getTableName());
                aggregationQueryBuilder.append("\n").append(cube.getIndentationString(1));
                aggregationQueryBuilder.append("SELECT ");
                for (Column col : combination) {
                    tempAggTable.addColumn(new Column(col));
                    aggregationQueryBuilder.append(col.getQuotedColumnName()).append(", ");
                }
                tempAggTable.addColumn(new Column(cube.getMeasure()));
                createTable.clear();
                tempAggTable.accept(createTable);

                aggregationQueryBuilder.append("SUM(").append(cube.getMeasure().getQuotedColumnName());
                aggregationQueryBuilder.append(") AS ").append(cube.getMeasure().getQuotedColumnName());
                aggregationQueryBuilder.append("\n").append(cube.getIndentationString(1));
                aggregationQueryBuilder.append("FROM ");
                if (cnt < dimensions.size()) {
                    // can reuse results
                    List<Table> tables = cube.getDatabase().getTables();
                    // search from the back to get optimal result
                    for (int i = tables.size() - 1; i >= 0; i--) {
                        Table t = tables.get(i);
                        if (! (t instanceof AggregationTempTable)) {
                            continue;
                        }
                        if (tempAggTable.canDeriveFrom((AggregationTempTable)t)) {
                            aggregationQueryBuilder.append(t.getTableName());
                            break;
                        }
                    }
                }
                else {
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
    }



}
