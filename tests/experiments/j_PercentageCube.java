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
    FactTableBuilder ftBuilderDelta = new FactTableBuilder("factTableDelta", 3);
    Table factTable = ftBuilder.getTable();
    Table factTableDelta = ftBuilderDelta.getTable();
    Database db = new Database();
    CreateTableQuerySet ct = new CreateTableQuerySet();
    DbConnection conn;

    public j_PercentageCube() throws ClassNotFoundException, SQLException {
        db.addTable(factTable);
        db.addTable(factTableDelta);
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
    public void evaluateCubeIncremental() throws SQLException {
        PercentageCube cube = new PercentageCube(db,
                new String[] {ftBuilder.getCubeParameter()});

        cube.evaluate();
        conn.executeQuerySet(cube);
        cube.evaluateIncrementallyOn(ftBuilderDelta.getTable());
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
    public void generateDeltaData() throws ClassNotFoundException, SQLException {

        int rowCount = 100;

        factTableDelta.accept(ct);
        conn.executeQuerySet(ct);
        ftBuilderDelta.populateData(rowCount, conn);
    }

    @Test
    public void generateData() throws ClassNotFoundException, SQLException {

        int rowCount = 1000;

        factTable.accept(ct);
        conn.executeQuerySet(ct);
        ftBuilder.populateData(rowCount, conn);
    }



}
