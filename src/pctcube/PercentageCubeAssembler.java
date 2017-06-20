package pctcube;

import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
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
        for (int numOfSelectedDimensions = cube.getDimensions().size();
                numOfSelectedDimensions >= 1;
                numOfSelectedDimensions--) {
            dimensionSelector.setNumOfElementsToSelect(numOfSelectedDimensions);
            for (List<Column> selection : dimensionSelector) {
                String aggIndvTableName = AggregationTempTable.getAggregationTempTableName(dimensionSelector.getCurrentSelectionFlags());
                PermutationGenerator<Column> pgen = new PermutationGenerator<>(selection);
                for (List<Column> permutation : pgen) {
                    for (int totalByKeyCount = 0; totalByKeyCount < selection.size(); totalByKeyCount++) {
                        queryBuilder.setLength(0);
                        queryBuilder.append("INSERT INTO ").append(cube.getTargetTable().getTableName());
                        queryBuilder.append("\n").append(cube.getIndentationString(1));
                        queryBuilder.append("SELECT '");

                        for (int i = 0; i < totalByKeyCount; i++) {
                            queryBuilder.append(permutation.get(i).getColumnName()).append(",");
                        }
                        if (totalByKeyCount > 0) {
                            queryBuilder.setLength(queryBuilder.length() - 1);
                        }
                        queryBuilder.append("', '");
                        for (int i = totalByKeyCount; i < permutation.size(); i++) {
                            queryBuilder.append(permutation.get(i).getColumnName()).append(",");
                        }
                        queryBuilder.setLength(queryBuilder.length() - 1);
                        queryBuilder.append("', ");
                        List<Integer> selectionFlag = dimensionSelector.getCurrentSelectionFlags();
                        // total level aggregation (a) join individual level aggregation (b) (smaller table join larger table)

                        for (int i = 0; i < selectionFlag.size(); i++) {
                            if (selectionFlag.get(i) > 0) {
                                queryBuilder.append("b.");
                                queryBuilder.append(dimensions.get(i).getQuotedColumnName());
                            }
                            else {
                                queryBuilder.append("NULL");
                            }
                            queryBuilder.append(", ");
                        }
                        queryBuilder.append("b.").append(cube.getMeasure().getQuotedColumnName());
                        queryBuilder.append(" / a.").append(cube.getMeasure().getQuotedColumnName());
                        queryBuilder.append(" AS ").append(cube.getMeasure().getQuotedColumnName());
                        queryBuilder.append("\n").append(cube.getIndentationString(1)).append("FROM ");

                        String totalLevelAggTableName = AggregationTempTable.TEMP_TABLE_PREFIX + pgen.getCurrentPermuationString(totalByKeyCount);
                        if (totalByKeyCount == 0) {
                            queryBuilder.append(PercentageCubeAggregateAction.TEMP_AGG_COUNT);
                        }
                        else if (cube.getDatabase().getTableByName(totalLevelAggTableName) != null) {
                            queryBuilder.append(totalLevelAggTableName);
                        }
                        else {
                            queryBuilder.append(cube.getFactTable().getTableName());
                        }
                        queryBuilder.append(" a JOIN ").append(aggIndvTableName).append(" b\n").append(cube.getIndentationString(2));
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
