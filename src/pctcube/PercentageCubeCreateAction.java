package pctcube;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

public class PercentageCubeCreateAction implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        // Create table, drop the old one if it exists.
        Table pctCubeTable = PercentageCubeTableFactory.getTable(cube);
        cube.getDatabase().addOrReplaceTable(pctCubeTable);
        CreateTableQuerySet ct = new CreateTableQuerySet();
        ct.setAddDropIfExists(true);
        pctCubeTable.accept(ct);

        cube.addAllQueries(ct.getQueries());
        cube.m_pctCubeTable = pctCubeTable;
    }
}
