package pctcube;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

public class PercentageCubeCreateAction implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        // Create table, drop the old one if it exists.
        Table pctCubeTable = PercentageCubeTableFactory.getTable(cube);
        Table olapCubeTable = OLAPCubeTableFactory.getTable(cube);
        Table topKCubeTable = null;
        Table topKTempTable = null;
        cube.getDatabase().addOrReplaceTable(pctCubeTable);
        cube.getDatabase().addOrReplaceTable(olapCubeTable);
        CreateTableQuerySet ct = new CreateTableQuerySet();
        ct.setAddDropIfExists(true);
        pctCubeTable.accept(ct);
        olapCubeTable.accept(ct);

        if (cube.getTopK() > 0) {
            topKCubeTable = new Table(pctCubeTable);
            topKTempTable = new Table(pctCubeTable);
            topKCubeTable.setTableName("pctTopK");
            topKTempTable.setTableName("pctTopKTemp");
            cube.getDatabase().addOrReplaceTable(topKCubeTable);
            cube.getDatabase().addOrReplaceTable(topKTempTable);
            topKCubeTable.accept(ct);
            topKTempTable.accept(ct);
        }

        cube.addAllQueries(ct.getQueries());
        cube.m_pctCubeTable = pctCubeTable;
        cube.m_olapCubeTable = olapCubeTable;
        cube.m_topKTargetTable = topKCubeTable;
        cube.m_topKTempTable = topKTempTable;
    }

}
