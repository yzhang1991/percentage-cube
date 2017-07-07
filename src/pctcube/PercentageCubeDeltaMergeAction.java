package pctcube;

import java.util.ArrayList;
import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.Table;
import pctcube.database.query.QuerySet;

public class PercentageCubeDeltaMergeAction implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        Table deltaOLAPCubeTable = cube.getDatabase().getTableByName("olap_cube_delta");
        Table olapCubeTable = cube.getOLAPCubeTable();
        if (deltaOLAPCubeTable == null) {
            return;
        }
        cube.addQuery(String.format("DROP TABLE IF EXISTS %s;", INTERMEDIATE_TEMP));
        StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ");
        queryBuilder.append(olapCubeTable.getTableName()).append(" RENAME TO ");
        queryBuilder.append(INTERMEDIATE_TEMP).append(";");
        cube.addQuery(queryBuilder.toString());

        queryBuilder.setLength(0);

        // Merge
        // The table schema will be [group by columns], cnt, m
        String measure = cube.getMeasure().getQuotedColumnName();
        List<Column> columns = olapCubeTable.getColumns();
        List<String> dimensionList = new ArrayList<>();
        for (int i = 0; i < columns.size() - 2; i++) {
            dimensionList.add(columns.get(i).getQuotedColumnName());
        }
        queryBuilder.append("SELECT ");
        queryBuilder.append(String.join(", ", dimensionList));
        // CNT
        queryBuilder.append(", SUM(cnt) AS cnt, ");
        // SUM(m)
        queryBuilder.append("SUM(").append(measure);
        queryBuilder.append(") AS ").append(measure).append("\nINTO ").append(olapCubeTable.getTableName());
        queryBuilder.append(" FROM (\n");
        queryBuilder.append(QuerySet.getIndentationString(1));
        queryBuilder.append("SELECT * FROM ").append(deltaOLAPCubeTable.getTableName());
        queryBuilder.append("\n").append(QuerySet.getIndentationString(1));
        queryBuilder.append(" UNION ALL\n");
        queryBuilder.append(QuerySet.getIndentationString(1));
        queryBuilder.append("SELECT * FROM ").append(INTERMEDIATE_TEMP).append(") a\n");

        queryBuilder.append("GROUP BY ").append(String.join(", ", dimensionList));
        queryBuilder.append(";");
        cube.addQuery(queryBuilder.toString());
//        cube.addQuery(String.format("DROP TABLE %s;", INTERMEDIATE_TEMP));
    }

    private static final String INTERMEDIATE_TEMP = "INTERMEDIATE_TEMP";

}
