package pctcube;

import java.util.ArrayList;
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

        String measureName = cube.getMeasure().getQuotedColumnName();
        List<Column> dimensions = cube.getDimensions();
        CombinationGenerator<Column> dimensionSelector = new CombinationGenerator<>(dimensions);
        // Select the dimensions that are not "ALL"s.

        for (int numOfSelectedDimensions = cube.getDimensions().size();
                numOfSelectedDimensions >= 1;
                numOfSelectedDimensions--) {

            dimensionSelector.setNumOfElementsToSelect(numOfSelectedDimensions);
            for (List<Column> selection : dimensionSelector) {

                List<Integer> selectionFlags = dimensionSelector.getCurrentSelectionFlags();
                List<String> unselectedDimensionNames = new ArrayList<>();
                List<String> selectedDimensionNames = new ArrayList<>();
                // The values for all the dimensions. If a dimension is not selected, use NULL.
                List<String> dimensionValues = new ArrayList<>();
                for (int i = 0; i < selectionFlags.size(); i++) {
                    String columnName = dimensions.get(i).getQuotedColumnName();
                    if (selectionFlags.get(i) == 0) {
                        // not selected.
                        unselectedDimensionNames.add(columnName);
                        dimensionValues.add("NULL");
                    }
                    else {
                        // selected.
                        selectedDimensionNames.add(columnName);
                        dimensionValues.add("b." + columnName);
                    }
                }

                // Exhaust all the possible orders of the selected dimensions.
                PermutationGenerator<Column> pgen = new PermutationGenerator<>(selection);
                for (List<Column> permutation : pgen) {

                    // Total-by key count varies from 0 (global aggregation) to dimension size minus one.
                    // This is because at least one dimension needs to be selected as the break-down-by key.
                    for (int totalByKeyCount = 0; totalByKeyCount < selection.size(); totalByKeyCount++) {

                        List<String> totalByColumnNames = new ArrayList<>();
                        List<String> breakdownByColumnNames = new ArrayList<>();
                        for (int i = 0; i < totalByKeyCount; i++) {
                            totalByColumnNames.add(permutation.get(i).getQuotedColumnName());
                        }
                        for (int i = totalByKeyCount; i < permutation.size(); i++) {
                            breakdownByColumnNames.add(permutation.get(i).getQuotedColumnName());
                        }

                        StringBuilder queryBuilder = new StringBuilder();

                        queryBuilder.append("INSERT INTO ");
                        queryBuilder.append(cube.getPercentageCubeTable().getTableName());
                        queryBuilder.append("\n").append(QuerySet.getIndentationString(1));

                        queryBuilder.append("SELECT '");
                        // Assemble the string in the "total by" column.
                        queryBuilder.append(String.join(",", totalByColumnNames));
                        queryBuilder.append("', '");

                        // Assemble the string in the "break down by" column.
                        queryBuilder.append(String.join(",", breakdownByColumnNames));
                        queryBuilder.append("', ");

                        // Add all the dimension values, add NULL if the dimension is not selected.
                        queryBuilder.append(String.join(", ", dimensionValues));

                        // Compute the percentage value.
                        queryBuilder.append(", b.").append(measureName);
                        queryBuilder.append(" / a.").append(measureName);
                        queryBuilder.append(" AS ").append(measureName);

                        queryBuilder.append(" FROM\n").append(QuerySet.getIndentationString(2));
                        // Total level aggregation (a) join individual level aggregation (b) (smaller table join larger table)
                        // Total level aggregation, group by total-by keys:
                        queryBuilder.append("(SELECT ");
                        queryBuilder.append(String.join(", ", totalByColumnNames));
                        if (totalByColumnNames.size() > 0) {
                            queryBuilder.append(", ");
                        }
                        queryBuilder.append("cnt, ").append(measureName);
                        queryBuilder.append(" FROM ").append(cube.getOLAPCubeTable().getTableName());
                        queryBuilder.append(" WHERE ").append(String.join(" IS NULL AND ", unselectedDimensionNames));
                        if (unselectedDimensionNames.size() > 0) {
                            queryBuilder.append(" IS NULL AND ");
                        }
                        queryBuilder.append(String.join(" IS NOT NULL AND ", totalByColumnNames));
                        if (totalByColumnNames.size() > 0) {
                            queryBuilder.append(" IS NOT NULL AND ");
                        }
                        queryBuilder.append(String.join(" IS NULL AND ", breakdownByColumnNames));
                        queryBuilder.append(" IS NULL) a JOIN\n").append(QuerySet.getIndentationString(2));

                        // Individual level aggregation, group by both total-by keys and breakdown-by keys:
                        queryBuilder.append("(SELECT ");
                        queryBuilder.append(String.join(", ", totalByColumnNames));
                        if (totalByColumnNames.size() > 0) {
                            queryBuilder.append(", ");
                        }
                        queryBuilder.append(String.join(", ", breakdownByColumnNames));
                        queryBuilder.append(", cnt, ").append(measureName);
                        queryBuilder.append(" FROM ").append(cube.getOLAPCubeTable().getTableName());
                        queryBuilder.append(" WHERE ").append(String.join(" IS NULL AND ", unselectedDimensionNames));
                        if (unselectedDimensionNames.size() > 0) {
                            queryBuilder.append(" IS NULL AND ");
                        }
                        queryBuilder.append(String.join(" IS NOT NULL AND ", totalByColumnNames));
                        if (totalByColumnNames.size() > 0) {
                            queryBuilder.append(" IS NOT NULL AND ");
                        }
                        queryBuilder.append(String.join(" IS NOT NULL AND ", breakdownByColumnNames));
                        queryBuilder.append(" IS NOT NULL) b ON\n").append(QuerySet.getIndentationString(2));

                        if (totalByColumnNames.size() == 0) {
                            queryBuilder.append("1 = 1");
                        }
                        else {
                            for (int i = 0; i < totalByColumnNames.size(); i++) {
                                String totalByColumnName = totalByColumnNames.get(i);
                                queryBuilder.append(String.format("a.%s = b.%s", totalByColumnName, totalByColumnName));
                                if (i < totalByColumnNames.size() - 1) {
                                    queryBuilder.append(" AND ");
                                }
                            }
                        }
                        queryBuilder.append(";");

//
//
//                        for (int i = 0; i < totalByKeyCount; i++) {
//                            String columnName = permutation.get(i).getQuotedColumnName();
//                            queryBuilder.append(columnName).append(",");
//                            totalByIsNullCondition.append(columnName).append(" IS %sNULL AND ");
//                        }
//                        if (totalByKeyCount > 0) {
//                            queryBuilder.setLength(queryBuilder.length() - 1);
//                        }
//
//
//
//                        for (int i = totalByKeyCount; i < permutation.size(); i++) {
//                            String columnName = permutation.get(i).getQuotedColumnName();
//                            queryBuilder.append(columnName).append(",");
//                            breakdownByIsNullCondition.append(columnName).append(" IS %sNULL AND ");
//                        }
//                        breakdownByIsNullCondition.setLength(breakdownByIsNullCondition.length() - 5);
//                        queryBuilder.setLength(queryBuilder.length() - 1);
//
//
//
//                        //
//                        queryBuilder.append(dimensionValues.toString());
//
//
//                        queryBuilder.append("(SELECT * FROM ").append(cube.getOLAPCubeTable().getTableName());
//                        queryBuilder.append(" WHERE ").append(unselectedDimensionIsNull.toString());
//                        queryBuilder.append(totalByIsNullCondition.toString().replaceAll("%s", "NOT "));
//                        queryBuilder.append(breakdownByIsNullCondition.toString().replaceAll("%s", ""));
//                        queryBuilder.append(") a JOIN\n").append(QuerySet.getIndentationString(1));
//                        queryBuilder.append("(SELECT * FROM ").append(cube.getOLAPCubeTable().getTableName());
//                        queryBuilder.append(" WHERE ").append(unselectedDimensionIsNull.toString());
//                        queryBuilder.append(totalByIsNullCondition.toString().replaceAll("%s", "NOT "));
//                        queryBuilder.append(breakdownByIsNullCondition.toString().replaceAll("%s", "NOT "));
//                        queryBuilder.append(";");
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
