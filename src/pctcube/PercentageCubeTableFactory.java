package pctcube;

import pctcube.database.Column;
import pctcube.database.DataType;
import pctcube.database.Table;

public class PercentageCubeTableFactory {

    public static Table getTable(PercentageCube cube) {
        Table retval = new Table("pct_cube");
        retval.addColumn(new Column("total by", DataType.VARCHAR).setNullable(false));
        retval.addColumn(new Column("break down by", DataType.VARCHAR).setNullable(false));
        for (Column dimension : cube.getDimensions()) {
            retval.addColumn(new Column(dimension));
        }
        Column measure = cube.getMeasure();
        Column percentageMeasure = new Column(measure.getColumnName() + "%", DataType.FLOAT);
        percentageMeasure.setNullable(false);
        retval.addColumn(percentageMeasure);
        return retval;
    }

}
