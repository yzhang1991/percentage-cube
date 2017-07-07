package pctcube;

import java.util.ArrayList;
import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;
import pctcube.utils.CombinationGenerator;
import pctcube.utils.PermutationGenerator;

public class PercentageCubeTopKFilter implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        if (cube.getTopK() == 0) {
            return;
        }

        Table fullCubeTable = cube.getPercentageCubeTable();
        List<Column> cubeColumns = fullCubeTable.getColumns();
        String totalByColumnName = cubeColumns.get(0).getQuotedColumnName();
        String breakdownByColumnName = cubeColumns.get(1).getQuotedColumnName();
        String measureName = cubeColumns.get(cubeColumns.size() - 1).getQuotedColumnName();

        Table topKResult = PercentageCubeTableFactory.getTable(cube);
        topKResult.setTableName("pct_cube_topk");
        CreateTableQuerySet ct = new CreateTableQuerySet().setAddDropIfExists(true);
        topKResult.accept(ct);
        cube.addAllQueries(ct.getQueries());

        // Select the dimensions that are not "ALL"s.
        List<Column> dimensions = cube.getDimensions();
        CombinationGenerator<Column> dimensionSelector = new CombinationGenerator<>(dimensions);
        for (int numOfSelectedDimensions = cube.getDimensions().size();
                numOfSelectedDimensions >= 1;
                numOfSelectedDimensions--) {

            dimensionSelector.setNumOfElementsToSelect(numOfSelectedDimensions);
            for (List<Column> selection : dimensionSelector) {

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

                        queryBuilder.append("INSERT INTO ").append(topKResult.getTableName());
                        queryBuilder.append("\nSELECT * FROM ").append(fullCubeTable.getTableName());
                        queryBuilder.append("\nWHERE ").append(totalByColumnName).append(" = '");
                        // Assemble the string in the "total by" column.
                        queryBuilder.append(String.join(",", totalByColumnNames));
                        queryBuilder.append("' AND ").append(breakdownByColumnName).append(" = '");
                        // Assemble the string in the "break down by" column.
                        queryBuilder.append(String.join(",", breakdownByColumnNames));
                        queryBuilder.append("' ORDER BY ").append(measureName).append(" DESC LIMIT ");
                        queryBuilder.append(cube.getTopK());
                        queryBuilder.append(";");
                        cube.addQuery(queryBuilder.toString());
                    }
                }
            }
        }

    }
}
