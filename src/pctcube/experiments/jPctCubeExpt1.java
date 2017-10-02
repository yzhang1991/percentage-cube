package pctcube.experiments;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import pctcube.FactTableBuilder;
import pctcube.PercentageCube;
import pctcube.database.Config;
import pctcube.database.Database;
import pctcube.database.DbConnection;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

// Supplemental experiment 1 for the revision:
// add NULL  1 - 5% in dimension
// n = 20M (specified in the config.ini), d = 3, NULL 1 - 5%
public class jPctCubeExpt1 {

    private static final int DIMENSION_COUNT = 3;
    private static final String FACT_TABLE_PREFIX = "FT_EXP1_d" + DIMENSION_COUNT + "_NUL";
    private static final String HORIZONTAL_RULE =
            "------------------------------------------------------------";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
        StringBuilder fullStatsBuilder = new StringBuilder();
        Config config = Config.getConfigFromFile("config.ini");
        // Try 1% to 5%
        for (int nullPercentage = 1; nullPercentage <= 5; nullPercentage++) {
            jPctCubeExpt1 experiment = new jPctCubeExpt1(config, nullPercentage);
            fullStatsBuilder.append(experiment.run()).append("\n");
        }
        if (fullStatsBuilder.length() > 0) {
            fullStatsBuilder.setLength(fullStatsBuilder.length() - 1);
        }
        printHeader("RESULT");
        printLogStatic("RESULT", fullStatsBuilder.toString());
        printLogStatic("RESULT", HORIZONTAL_RULE);
    }

    private int m_nullPercentage;

    private Database m_database = new Database();
    private FactTableBuilder m_factTableBuilder;
    private Table m_factTable;
    private PercentageCube m_cube;
    private Config m_config;
    private DbConnection m_connection;

    public jPctCubeExpt1(Config config, int nullPercentage) throws ClassNotFoundException, SQLException, FileNotFoundException {
        m_nullPercentage = nullPercentage;
        // Build the fact tables.
        m_factTableBuilder = new FactTableBuilder(FACT_TABLE_PREFIX + m_nullPercentage, DIMENSION_COUNT);
        m_factTable = m_factTableBuilder.getTable();
        // Add those tables to the program-maintained database catalog, so the query generator
        // can be aware of their existences.
        m_database.addTable(m_factTable);

        m_cube = new PercentageCube(m_database,
                new String[] {m_factTableBuilder.getCubeParameter()});

        m_config = config;
        m_connection = new DbConnection(config);
    }

    private static void printHeader(String className) {
        printLogStatic(className, HORIZONTAL_RULE);
        printLogStatic(className, "%8s%8s%8s%10s",
                "n", "d", "null%", "original");
        printLogStatic(className, HORIZONTAL_RULE);
    }

    public String run() throws ClassNotFoundException, SQLException {
        if (m_config.needToGenerateData()) {
            generateData();
        }
        double executionTime = runExpt();
        String retData = String.format("%8d%8d%8d%10.2f",
                m_config.getDataSize(), 3, m_nullPercentage, executionTime);
        printHeader(this.getClass().getSimpleName());
        printLog(retData);
        printLog(HORIZONTAL_RULE);
        return retData;
    }

    private double runExpt() throws SQLException {
        printLog("Start building the cube.");
        long startTime = System.currentTimeMillis();

        m_cube.evaluate();
        m_connection.executeQuerySet(m_cube);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        m_connection.execute("SELECT CLEAR_CACHES();");
        return duration;
    }


    private void generateData() throws SQLException, ClassNotFoundException {
        printLog("Generate data n = %d, d = %d, %d%% NULL.",
                m_config.getDataSize(), DIMENSION_COUNT, m_nullPercentage);
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);

        m_factTable.accept(createTableQuerySet);

        createTableQuerySet.addQuery(m_factTableBuilder.getProjectionDDL());
        m_connection.executeQuerySet(createTableQuerySet);

        m_factTableBuilder.populateData(m_config.getDataSize(), m_nullPercentage, 0, m_connection);
    }

    private static final DateTimeFormatter DT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    private static void printLogStatic(String className, String msg, Object...args) {
        if (args != null) {
            msg = String.format(msg, args);
        }

        String header = String.format("%s [%s] ",
                ZonedDateTime.now().format(DT_FORMAT),
                className);

        System.out.println(String.format("%s%s", header, msg.replaceAll("\n", "\n" + header)));
    }

    private void printLog(String msg, Object...args) {
        printLogStatic(this.getClass().getSimpleName(), msg, args);
    }

}
