package pctcube;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

public class PercentageCubeCreateAction implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        // Create table, drop the old one if it exists.
        Table cubeTable = PercentageCubeTableFactory.getTable(cube);
        Table topKCubeTable = null;
        Table topKTempTable = null;
        cube.getDatabase().addOrReplaceTable(cubeTable);
        CreateTableQuerySet ct = new CreateTableQuerySet();
        ct.setAddDropIfExists(true);
        cubeTable.accept(ct);

        if (cube.getTopK() > 0) {
            topKCubeTable = new Table(cubeTable);
            topKTempTable = new Table(cubeTable);
            topKCubeTable.setTableName("pctTopK");
            topKTempTable.setTableName("pctTopKTemp");
            cube.getDatabase().addOrReplaceTable(topKCubeTable);
            cube.getDatabase().addOrReplaceTable(topKTempTable);
            topKCubeTable.accept(ct);
            topKTempTable.accept(ct);
        }

        cube.addAllQueries(ct.getQueries());
        cube.m_targetTable = cubeTable;
        cube.m_topKTargetTable = topKCubeTable;
        cube.m_topKTempTable = topKTempTable;
    }

}
