package pctcube;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import pctcube.database.Config;
import pctcube.database.Database;
import pctcube.database.DbConnection;
import pctcube.database.Table;
import pctcube.database.TempTableCleanupAction;
import pctcube.database.query.CreateTableQuerySet;

public class jPctCubeExperiment {

    private static final String FACT_TABLE_ORIGINAL = "factTableOriginal_d";
    private static final String FACT_TABLE_DELTA = "factTableDelta_d";
    private static final String FACT_TABLE_FINAL = "factTableFinal_d";
    private static final String HORIZONTAL_RULE =
            "------------------------------------------------------------";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
        StringBuilder fullStatsBuilder = new StringBuilder();
        Config config = Config.getConfigFromFile("config.ini");
        for (int dimensionCount = config.getDStart();
                dimensionCount <= config.getDEnd(); dimensionCount++) {
            jPctCubeExperiment experiment = new jPctCubeExperiment(config, dimensionCount);
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
    private FactTableBuilder m_factTableOriginalBuilder;
    private FactTableBuilder m_factTableDeltaBuilder;
    private FactTableBuilder m_factTableFinalBuilder;
    private Table m_factTableOriginal;
    private Table m_factTableDelta;
    private Table m_factTableFinal;
    private PercentageCube m_cubeOriginal;
    private PercentageCube m_cubeFinal;
    private PercentageCube m_cubeTopK;
    private Config m_config;
    private DbConnection m_connection;

    public jPctCubeExperiment(Config config, int dimensionCount) throws ClassNotFoundException, SQLException, FileNotFoundException {
        m_dimensionCount = dimensionCount;
        // Build the fact tables.
        m_factTableOriginalBuilder = new FactTableBuilder(FACT_TABLE_ORIGINAL + m_dimensionCount, m_dimensionCount);
        m_factTableDeltaBuilder = new FactTableBuilder(FACT_TABLE_DELTA + m_dimensionCount, m_dimensionCount);
        m_factTableFinalBuilder = new FactTableBuilder(FACT_TABLE_FINAL + m_dimensionCount, m_dimensionCount);
        m_factTableOriginal = m_factTableOriginalBuilder.getTable();
        m_factTableDelta = m_factTableDeltaBuilder.getTable();
        m_factTableFinal = m_factTableFinalBuilder.getTable();
        // Add those tables to the program-maintained database catalog, so the query generator
        // can be aware of their existences.
        m_database.addTables(m_factTableOriginal, m_factTableDelta, m_factTableFinal);

        m_cubeOriginal = new PercentageCube(m_database,
                new String[] {m_factTableOriginalBuilder.getCubeParameter()});
        m_cubeFinal = new PercentageCube(m_database,
                new String[] {m_factTableFinalBuilder.getCubeParameter()});
        m_cubeTopK = new PercentageCube(m_database,
                new String[] {m_factTableOriginalBuilder.getCubeParameter(), "topk=2"});

        m_config = config;
        m_connection = new DbConnection(config);
    }

    private static void printHeader(String className) {
        printLogStatic(className, HORIZONTAL_RULE);
        printLogStatic(className, "%8s%8s%10s%10s%10s%10s",
                "n", "d", "original", "delta", "holistic", "top-k=2");
        printLogStatic(className, HORIZONTAL_RULE);
    }

    public String run() throws ClassNotFoundException, SQLException {
        if (m_config.needToGenerateData()) {
            generateData();
        }
        double originalTime = runOriginal();
        // Do not clean up here, results need to be reused.
        double incrTime = runIncremental();
        cleanup();
        double finalTime = runFinal();
        cleanup();
        double topKTime = runWithTopK();
        cleanup();
        String retData = String.format("%8d%8d%10.2f%10.2f%10.2f%10.2f",
                m_config.getDataSize(), m_dimensionCount, originalTime, incrTime, finalTime, topKTime);
        printHeader(this.getClass().getSimpleName());
        printLog(retData);
        printLog(HORIZONTAL_RULE);
        return retData;
    }

    // Build the original cube.
    private double runOriginal() throws SQLException {
        printLog("Start building the orignal cube.");
        long startTime = System.currentTimeMillis();

        m_cubeOriginal.evaluate();
        m_connection.executeQuerySet(m_cubeOriginal);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        return duration;
    }

    // Evaluate the cube using an incremental delta fact table.
    private double runIncremental() throws SQLException {
        printLog("Build the incremental cube.");
        long startTime = System.currentTimeMillis();

        m_cubeOriginal.evaluateIncrementallyOn(m_factTableDelta);
        m_connection.executeQuerySet(m_cubeOriginal);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        return duration;
    }

    // Evaluate the cube from scratch on a final fact table, which unions the data from both
    // the original fact table and the delta fact table.
    private double runFinal() throws SQLException {
        printLog("Build the final cube from scratch");
        long startTime = System.currentTimeMillis();

        m_cubeFinal.evaluate();
        m_connection.executeQuerySet(m_cubeFinal);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        return duration;
    }

    // Build the original cube using a top-K parameter.
    private double runWithTopK() throws SQLException {
        printLog("Build the orignal cube with topk=2");
        long startTime = System.currentTimeMillis();

        m_cubeTopK.evaluate();
        m_connection.executeQuerySet(m_cubeTopK);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        return duration;
    }

    private void cleanup() throws SQLException {
        printLog("Cleanup the temp tables.");
        TempTableCleanupAction cleaner = new TempTableCleanupAction();
        m_database.accept(cleaner);
        m_connection.executeQuerySet(cleaner);
    }

    private void generateData() throws SQLException, ClassNotFoundException {
        printLog("Generate data n = %d (original), %d (delta), d = %d.",
                m_config.getDataSize(), m_config.getDeltaDataSize(), m_dimensionCount);
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);

        m_factTableOriginal.accept(createTableQuerySet);
        m_factTableDelta.accept(createTableQuerySet);
        m_factTableFinal.accept(createTableQuerySet);

        m_connection.executeQuerySet(createTableQuerySet);

        m_factTableOriginalBuilder.populateData(m_config.getDataSize(), m_connection);
        m_factTableDeltaBuilder.populateData(m_config.getDeltaDataSize(), m_connection);
        String populateFinalFactTableQueryTemplate =
                "INSERT INTO %s SELECT * FROM %s;";
        m_connection.execute(String.format(populateFinalFactTableQueryTemplate,
                m_factTableFinal.getTableName(), m_factTableOriginal.getTableName()));
        m_connection.execute(String.format(populateFinalFactTableQueryTemplate,
                m_factTableFinal.getTableName(), m_factTableDelta.getTableName()));
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
