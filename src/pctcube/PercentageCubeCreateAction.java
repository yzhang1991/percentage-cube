package pctcube;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

public class PercentageCubeCreateAction implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
        // Create table, drop the old one if it exists.
        Table cubeTable = PercentageCubeTableFactory.getTable(cube);
        cube.getDatabase().dropTable(cubeTable);
        cube.getDatabase().addTable(cubeTable);
        CreateTableQuerySet ct = new CreateTableQuerySet();
        ct.setAddDropIfExists(true);
        cubeTable.accept(ct);
        cube.addAllQueries(ct.getQueries());
        cube.setTargetTable(cubeTable);
    }

}
