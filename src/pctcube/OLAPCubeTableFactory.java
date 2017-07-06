package pctcube;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Table;

public class OLAPCubeTableFactory {

    public static Table getTable(PercentageCube cube) {
        Table retval = new Table("olap_cube");
        for (Column dimension : cube.getDimensions()) {
            retval.addColumn(new Column(dimension));
        }
        Column count = new Column("cnt", DataType.INTEGER).setNullable(false);
        Column measure = new Column(cube.getMeasure()).setNullable(false);
        retval.addColumn(count);
        retval.addColumn(measure);
        return retval;
    }
}
