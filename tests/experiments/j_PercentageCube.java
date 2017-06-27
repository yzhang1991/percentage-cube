package experiments;

import java.sql.SQLException;

import org.junit.Test;

import pctcube.FactTableBuilder;
import pctcube.PercentageCube;
import pctcube.database.Database;
import pctcube.database.DbConnection;
import pctcube.database.JDBCConfig;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

public class j_PercentageCube {

    FactTableBuilder ftBuilder = new FactTableBuilder("factTable", 3);
    Table factTable = ftBuilder.getTable();
    Database db = new Database();
    CreateTableQuerySet ct = new CreateTableQuerySet();
    DbConnection conn;

    public j_PercentageCube() throws ClassNotFoundException, SQLException {
        db.addTable(factTable);
        ct.setAddDropIfExists(true);
        conn = new DbConnection(new JDBCConfig());
    }

    @Test
    public void evaluateCube() throws SQLException {
        PercentageCube cube = new PercentageCube(db,
                new String[] {ftBuilder.getCubeParameter()});

        cube.evaluate();
        conn.executeQuerySet(cube);
    }

    @Test
    public void evaluateCubeTopK() throws SQLException {
        PercentageCube cube = new PercentageCube(db,
                new String[] {ftBuilder.getCubeParameter(), "topk=2"});

        cube.evaluate();
        conn.executeQuerySet(cube);
    }


    @Test
    public void generateData() throws ClassNotFoundException, SQLException {

        int rowCount = 1000;

        db.accept(ct);
        conn.executeQuerySet(ct);
        ftBuilder.populateData(rowCount, conn);
    }



}
