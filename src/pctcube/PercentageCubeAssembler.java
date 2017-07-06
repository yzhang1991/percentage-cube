package pctcube;

import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.query.QuerySet;
import pctcube.utils.CombinationGenerator;
import pctcube.utils.PermutationGenerator;

public class PercentageCubeAssembler implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        if (cube.getEvaluationMethod() == EvaluationMethod.GROUPBY) {
            assembleGroupBy(cube);
        }
        else {
            assembleOLAP(cube);
        }
    }

    private void assembleGroupBy(PercentageCube cube) {
        StringBuilder queryBuilder = new StringBuilder();
        List<Column> dimensions = cube.getDimensions();
        CombinationGenerator<Column> dimensionSelector = new CombinationGenerator<>(dimensions);
        // Select the dimensions that are not "ALL"s.
        for (int numOfSelectedDimensions = cube.getDimensions().size();
                numOfSelectedDimensions >= 1;
                numOfSelectedDimensions--) {
            dimensionSelector.setNumOfElementsToSelect(numOfSelectedDimensions);
            for (List<Column> selection : dimensionSelector) {
                // Get the pre-computed individual level aggregation result table name.
                // This result will be reused for a lot of times here.
                String aggIndvTableName = AggregationTempTable.getAggregationTempTableName(dimensionSelector.getCurrentSelectionFlags());
                List<Integer> selectedIndices = dimensionSelector.getCurrentSelectionIndices();
                // Exhaust all the possible orders of the selected dimensions.
                PermutationGenerator<Column> pgen = new PermutationGenerator<>(selection);
                for (List<Column> permutation : pgen) {
                    // Total-by key count varies from 0 (global aggregation) to dimension size minus one.
                    // This is because at least one dimension needs to be selected as the break-down-by key.
                    for (int totalByKeyCount = 0; totalByKeyCount < selection.size(); totalByKeyCount++) {
                        queryBuilder.setLength(0);
                        queryBuilder.append("INSERT INTO ").append(cube.getPercentageCubeTable().getTableName());
                        queryBuilder.append("\n").append(QuerySet.getIndentationString(1));
                        queryBuilder.append("SELECT '");

                        // If the selected dimension permutation is in ascending order, we can reuse the pre-computed aggregation
                        // result as the total level aggregation result as well.
                        // Try to figure out if this is possible by assembling a aggregation result table name
                        // and query the database catalog.
                        StringBuilder totalLevelAggTableName = new StringBuilder(AggregationTempTable.TEMP_TABLE_PREFIX);
                        for (int i = 0; i < totalByKeyCount; i++) {
                            // Assemble the string in the "total by" column.
                            queryBuilder.append(permutation.get(i).getColumnName()).append(",");
                            totalLevelAggTableName.append(selectedIndices.get(pgen.getElementIndexAtPosition(i))).append("_");
                        }
                        if (totalByKeyCount > 0) {
                            queryBuilder.setLength(queryBuilder.length() - 1);
                            totalLevelAggTableName.setLength(totalLevelAggTableName.length() - 1);
                        }
                        queryBuilder.append("', '");
                        for (int i = totalByKeyCount; i < permutation.size(); i++) {
                            // Assemble the string in the "break down by" column.
                            queryBuilder.append(permutation.get(i).getColumnName()).append(",");
                        }
                        queryBuilder.setLength(queryBuilder.length() - 1);
                        queryBuilder.append("', ");

                        // Now add the values for all the dimensions. If a dimension is not selected, use NULL.
                        List<Integer> selectionFlag = dimensionSelector.getCurrentSelectionFlags();
                        // Total level aggregation (a) join individual level aggregation (b) (smaller table join larger table)
                        for (int i = 0; i < selectionFlag.size(); i++) {
                            if (selectionFlag.get(i) > 0) {
                                // selected.
                                queryBuilder.append("b.");
                                queryBuilder.append(dimensions.get(i).getQuotedColumnName());
                            }
                            else {
                                // not selected.
                                queryBuilder.append("NULL");
                            }
                            queryBuilder.append(", ");
                        }
                        // Compute the percentage value.
                        queryBuilder.append("b.").append(cube.getMeasure().getQuotedColumnName());
                        queryBuilder.append(" / a.").append(cube.getMeasure().getQuotedColumnName());
                        queryBuilder.append(" AS ").append(cube.getMeasure().getQuotedColumnName());
                        queryBuilder.append("\n").append(QuerySet.getIndentationString(1)).append("FROM ");

                        if (totalByKeyCount == 0) {
                            // If there is no total by column, then use the global count.
                            queryBuilder.append(PercentageCubeAggregateAction.TEMP_AGG_COUNT);
                        }
                        else if (cube.getDatabase().getTableByName(totalLevelAggTableName.toString()) != null) {
                            // If we can reuse the aggregation result for the total level aggregation, do it!
                            queryBuilder.append(totalLevelAggTableName);
                        }
                        else {
                            // No cached aggregate result can be used, create a nested query.
                            // We don't cache this because it is rarely reused.
                            queryBuilder.append("(SELECT ");
                            for (int i = 0; i < totalByKeyCount; i++) {
                                queryBuilder.append(permutation.get(i).getColumnName()).append(", ");
                            }
                            queryBuilder.append("COUNT(*) AS cnt, SUM(").append(cube.getMeasure().getQuotedColumnName());
                            queryBuilder.append(") AS ").append(cube.getMeasure().getQuotedColumnName());
                            queryBuilder.append(" FROM ").append(cube.getFactTable().getTableName());
                            queryBuilder.append(" GROUP BY ");
                            for (int i = 0; i < totalByKeyCount; i++) {
                                queryBuilder.append(permutation.get(i).getColumnName()).append(", ");
                            }
                            queryBuilder.setLength(queryBuilder.length() - 2);
                            queryBuilder.append(")");
                        }

                        queryBuilder.append(" a JOIN ").append(aggIndvTableName).append(" b\n");
                        queryBuilder.append(QuerySet.getIndentationString(2));
                        queryBuilder.append("ON ");
                        if (totalByKeyCount == 0) {
                            queryBuilder.append("1 = 1");
                        }
                        else {
                            for (int i = 0; i < totalByKeyCount; i++) {
                                queryBuilder.append("a.").append(permutation.get(i).getQuotedColumnName()).append(" = ");
                                queryBuilder.append("b.").append(permutation.get(i).getQuotedColumnName());
                                if (i != totalByKeyCount - 1) {
                                    queryBuilder.append(" AND ");
                                }
                            }
                        }
                        // Get top K percentages.
                        if (cube.getTopK() > 0) {
                            queryBuilder.append(" ORDER BY " ).append(cube.getMeasure().getQuotedColumnName());
                            queryBuilder.append(" DESC LIMIT ").append(cube.getTopK());
                        }
                        queryBuilder.append(";");
                        cube.addQuery(queryBuilder.toString());
                    }
                }
            }
        }
    }

    private void assembleOLAP(PercentageCube cube) {
        StringBuilder queryBuilder = new StringBuilder();
        cube.addQuery(queryBuilder.toString());
    }
}
