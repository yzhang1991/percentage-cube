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

// Supplemental experiment 2 for the revision:
// Try to prove that using row count threshold is faster than top-k
// n = 20M (specified in the config.ini), d = 1 - 5, threshold vs. top-k=2
public class jPctCubeExpt2 {

    private static final String FACT_TABLE_PREFIX = "FT_EXP2_d";
    private static final String HORIZONTAL_RULE =
            "------------------------------------------------------------";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
        StringBuilder fullStatsBuilder = new StringBuilder();
        Config config = Config.getConfigFromFile("config.ini");
        // Try d = 1 to 5
        for (int dimensionCount = 1; dimensionCount <= 5; dimensionCount++) {
            jPctCubeExpt2 experiment = new jPctCubeExpt2(config, dimensionCount);
            fullStatsBuilder.append(experiment.run()).append("\n");
        }
        if (fullStatsBuilder.length() > 0) {
            fullStatsBuilder.setLength(fullStatsBuilder.length() - 1);
        }
        printHeader("RESULT");
        printLogStatic("RESULT", fullStatsBuilder.toString());
        printLogStatic("RESULT", HORIZONTAL_RULE);
    }

    private int m_dimensionCount;

    private Database m_database = new Database();
    private FactTableBuilder m_factTableBuilder;
    private Table m_factTable;
    private PercentageCube m_cubeWithThreshold;
    private PercentageCube m_cubeWithTopK;
    private Config m_config;
    private DbConnection m_connection;

    public jPctCubeExpt2(Config config, int nullPercentage) throws ClassNotFoundException, SQLException, FileNotFoundException {
        m_dimensionCount = nullPercentage;
        // Build the fact tables.
        m_factTableBuilder = new FactTableBuilder(FACT_TABLE_PREFIX + m_dimensionCount, m_dimensionCount);
        m_factTable = m_factTableBuilder.getTable();
        // Add those tables to the program-maintained database catalog, so the query generator
        // can be aware of their existences.
        m_database.addTable(m_factTable);

        m_cubeWithThreshold = new PercentageCube(m_database,
                new String[] {m_factTableBuilder.getCubeParameter(), "rowcount=50"});
        m_cubeWithTopK = new PercentageCube(m_database,
                new String[] {m_factTableBuilder.getCubeParameter(), "topk=2"});

        m_config = config;
        m_connection = new DbConnection(config);
    }

    private static void printHeader(String className) {
        printLogStatic(className, HORIZONTAL_RULE);
        printLogStatic(className, "%8s%8s%10s%10s",
                "n", "d", "threshold", "top-k=2");
        printLogStatic(className, HORIZONTAL_RULE);
    }

    public String run() throws ClassNotFoundException, SQLException {
        if (m_config.needToGenerateData()) {
            generateData();
        }
        double thresholdTime = runThreshold();
        double topKTime = runTopK();
        String retData = String.format("%8d%8d%10.2f%10.2f",
                m_config.getDataSize(), m_dimensionCount, thresholdTime, topKTime);
        printHeader(this.getClass().getSimpleName());
        printLog(retData);
        printLog(HORIZONTAL_RULE);
        return retData;
    }

    private double runThreshold() throws SQLException {
        printLog("Start building the cube with thresdhold.");
        long startTime = System.currentTimeMillis();

        m_cubeWithThreshold.evaluate();
        m_connection.executeQuerySet(m_cubeWithThreshold);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        m_connection.execute("SELECT CLEAR_CACHES();");
        return duration;
    }

    private double runTopK() throws SQLException {
        printLog("Start building the cube with top-k");
        long startTime = System.currentTimeMillis();

        m_cubeWithTopK.evaluate();
        m_connection.executeQuerySet(m_cubeWithTopK);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        m_connection.execute("SELECT CLEAR_CACHES();");
        return duration;
    }

    private void generateData() throws SQLException, ClassNotFoundException {
        printLog("Generate data n = %d d = %d.",
                m_config.getDataSize(), m_dimensionCount);
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);

        m_factTable.accept(createTableQuerySet);

        createTableQuerySet.addQuery(m_factTableBuilder.getProjectionDDL());
        m_connection.executeQuerySet(createTableQuerySet);

        m_factTableBuilder.populateData(m_config.getDataSize(), 0, 0, m_connection);
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
