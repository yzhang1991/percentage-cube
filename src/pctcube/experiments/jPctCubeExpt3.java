package pctcube.experiments;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import pctcube.FactTableBuilder;
import pctcube.PercentageCube;
import pctcube.PercentageCubeAggregateAction;
import pctcube.PercentageCubeCreateAction;
import pctcube.database.Config;
import pctcube.database.Database;
import pctcube.database.DbConnection;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

// Supplemental experiment 3 for the revision:
// Run an incremental query with dimension cardinality varying in 1, 2, 4, and 8.
// n = 20M (specified in the config.ini), d = 4, original, incremental, full
public class jPctCubeExpt3 {

    private static final int DIMENSION_COUNT = 3;
    private static final String FACT_TABLE_ORIGINAL = "FT_EXP3_Original_d" + DIMENSION_COUNT + "_c";
    private static final String FACT_TABLE_DELTA = "FT_EXP3_Delta_d" + DIMENSION_COUNT + "_c";
    private static final String FACT_TABLE_FINAL = "FT_EXP3_Final_d" + DIMENSION_COUNT + "_c";
    private static final String HORIZONTAL_RULE =
            "------------------------------------------------------------";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
        StringBuilder fullStatsBuilder = new StringBuilder();
        int[] cardinalities = new int[] {1, 2, 4, 8};
        Config config = Config.getConfigFromFile("config.ini");
        for (int cardinality : cardinalities) {
            jPctCubeExpt3 experiment = new jPctCubeExpt3(config, cardinality);
            fullStatsBuilder.append(experiment.run()).append("\n");
        }
        if (fullStatsBuilder.length() > 0) {
            fullStatsBuilder.setLength(fullStatsBuilder.length() - 1);
        }
        printHeader("RESULT");
        printLogStatic("RESULT", fullStatsBuilder.toString());
        printLogStatic("RESULT", HORIZONTAL_RULE);
    }

    private int m_cardinality;
    private Database m_database = new Database();
    private FactTableBuilder m_factTableOriginalBuilder;
    private FactTableBuilder m_factTableDeltaBuilder;
    private FactTableBuilder m_factTableFinalBuilder;
    private Table m_factTableOriginal;
    private Table m_factTableDelta;
    private Table m_factTableFinal;
    private PercentageCube m_cubeOriginal;
    private PercentageCube m_cubeFinal;
    private Config m_config;
    private DbConnection m_connection;

    public jPctCubeExpt3(Config config, int cardinality) throws ClassNotFoundException, SQLException, FileNotFoundException {
        m_cardinality = cardinality;
        // Build the fact tables.
        m_factTableOriginalBuilder = new FactTableBuilder(FACT_TABLE_ORIGINAL + m_cardinality, DIMENSION_COUNT);
        m_factTableDeltaBuilder = new FactTableBuilder(FACT_TABLE_DELTA + m_cardinality, DIMENSION_COUNT);
        m_factTableFinalBuilder = new FactTableBuilder(FACT_TABLE_FINAL + m_cardinality, DIMENSION_COUNT);
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

        m_config = config;
        m_connection = new DbConnection(config);
    }

    private static void printHeader(String className) {
        printLogStatic(className, HORIZONTAL_RULE);
        printLogStatic(className, "%8s%8s%12s%10s%10s%10s",
                "n", "d", "cardinality", "original", "delta", "full");
        printLogStatic(className, HORIZONTAL_RULE);
    }

    public String run() throws ClassNotFoundException, SQLException {
        if (m_config.needToGenerateData()) {
            generateData();
        }
        double originalTime = runOriginal();
        double incrTime = runIncremental();
        double finalTime = runFinal();
        String retData = String.format("%8d%8d%12d%10.2f%10.2f%10.2f",
                m_config.getDataSize(), DIMENSION_COUNT, m_cardinality,
                originalTime, incrTime, finalTime);
        printHeader(this.getClass().getSimpleName());
        printLog(retData);
        printLog(HORIZONTAL_RULE);
        return retData;
    }

    // Build the original cube.
    private double runOriginal() throws SQLException {
        printLog("Start building the orignal cube.");
        long startTime = System.currentTimeMillis();

        m_cubeOriginal.clear();
        m_cubeOriginal.accept(new PercentageCubeCreateAction());
        m_cubeOriginal.accept(new PercentageCubeAggregateAction());
        m_connection.executeQuerySet(m_cubeOriginal);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        m_connection.execute("SELECT CLEAR_CACHES();");
        return duration;
    }

    // Evaluate the cube using an incremental delta fact table.
    private double runIncremental() throws SQLException {
        printLog("Build the incremental cube.");
        long startTime = System.currentTimeMillis();

        m_cubeOriginal.clear();
        m_cubeOriginal.accept(new PercentageCubeCreateAction());
        m_cubeOriginal.accept(new PercentageCubeAggregateAction(m_factTableDelta));
        m_connection.executeQuerySet(m_cubeOriginal);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        m_connection.execute("SELECT CLEAR_CACHES();");
        return duration;
    }

    // Evaluate the cube from scratch on a final fact table, which unions the data from both
    // the original fact table and the delta fact table.
    private double runFinal() throws SQLException {
        printLog("Build the final cube from scratch");
        long startTime = System.currentTimeMillis();

        m_cubeFinal.clear();
        m_cubeFinal.accept(new PercentageCubeCreateAction());
        m_cubeFinal.accept(new PercentageCubeAggregateAction());
        m_connection.executeQuerySet(m_cubeFinal);

        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        printLog("Finished in %.2f seconds.", duration);
        m_connection.execute("SELECT CLEAR_CACHES();");
        return duration;
    }

    private void generateData() throws SQLException, ClassNotFoundException {
        printLog("Generate data n = %d (original), %d (delta), cardinality = %d.",
                m_config.getDataSize(), m_config.getDeltaDataSize(), m_cardinality);
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);

        m_factTableOriginal.accept(createTableQuerySet);
        m_factTableDelta.accept(createTableQuerySet);
        m_factTableFinal.accept(createTableQuerySet);

        createTableQuerySet.addQuery(m_factTableOriginalBuilder.getProjectionDDL());
        createTableQuerySet.addQuery(m_factTableDeltaBuilder.getProjectionDDL());
        createTableQuerySet.addQuery(m_factTableFinalBuilder.getProjectionDDL());

        m_connection.executeQuerySet(createTableQuerySet);

        m_factTableOriginalBuilder.populateData(m_config.getDataSize(), 0, m_cardinality, m_connection);
        m_factTableDeltaBuilder.populateData(m_config.getDeltaDataSize(), 0, m_cardinality, m_connection);
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
