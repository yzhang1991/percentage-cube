package pctcube;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;
import pctcube.database.query.QuerySet;

public class PercentageCubeAggregateAction implements PercentageCubeVisitor {

    private Table m_deltaFactTable = null;

    public PercentageCubeAggregateAction() {

    }

    public PercentageCubeAggregateAction(Table deltaFactTable) {
        m_deltaFactTable = deltaFactTable;
    }

    // Evaluate all the aggregations that can be re-used during cube evaluation.
    @Override
    public void visit(PercentageCube cube) {
        StringBuilder aggregationQueryBuilder = new StringBuilder();
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);
        boolean delta = m_deltaFactTable != null;

        Table factTable = delta ? m_deltaFactTable : cube.getFactTable();
        Table olapCubeTable = OLAPCubeTableFactory.getTable(cube);
        if (delta) {
            olapCubeTable.setTableName(olapCubeTable.getTableName() + "_delta");
        }
        else {
            // If this is not a delta OLAP cube table, associate it with the percentage cube, it will be used later.
            cube.m_olapCubeTable = olapCubeTable;
        }
        olapCubeTable.accept(createTableQuerySet);
        cube.getDatabase().addOrReplaceTable(olapCubeTable);

        StringBuilder dimensionList = new StringBuilder();
        for (Column dimension : cube.getDimensions()) {
            dimensionList.append(dimension.getQuotedColumnName()).append(", ");
        }
        dimensionList.setLength(dimensionList.length() - 2);

        aggregationQueryBuilder.append("INSERT INTO ").append(olapCubeTable.getTableName()).append("\n");
        aggregationQueryBuilder.append(QuerySet.getIndentationString(1));
        aggregationQueryBuilder.append("SELECT ").append(dimensionList.toString());
        aggregationQueryBuilder.append(", COUNT(*), SUM(").append(cube.getMeasure().getQuotedColumnName());
        aggregationQueryBuilder.append(")\n").append(QuerySet.getIndentationString(1));
        aggregationQueryBuilder.append("FROM ").append(factTable.getTableName()).append("\n");
        aggregationQueryBuilder.append(QuerySet.getIndentationString(1));
        aggregationQueryBuilder.append("GROUP BY CUBE(").append(dimensionList.toString()).append(");");

        cube.addAllQueries(createTableQuerySet.getQueries());
        cube.addQuery(aggregationQueryBuilder.toString());
    }
}
