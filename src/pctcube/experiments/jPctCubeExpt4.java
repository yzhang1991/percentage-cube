package pctcube.experiments;

import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import pctcube.FactTableBuilder;
import pctcube.database.Config;
import pctcube.database.Database;
import pctcube.database.DbConnection;
import pctcube.database.Table;
import pctcube.database.query.CreateTableQuerySet;

// Generate the data for table 8.
public class jPctCubeExpt4 {

    private static final int DIMENSION_COUNT = 5;
    private static final String FACT_TABLE = "FT_EXP4";
    private static final String HORIZONTAL_RULE =
            "------------------------------------------------------------";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
        Config config = Config.getConfigFromFile("config.ini");
        jPctCubeExpt4 experiment = new jPctCubeExpt4(config);
        experiment.run();
    }

    private int[] m_cardinalities = new int[] {100, 1000, 10000, 100000, 10};
    private Database m_database = new Database();
    private FactTableBuilder m_factTableBuilder;
    private Table m_factTable;
    private Config m_config;
    private DbConnection m_connection;

    public jPctCubeExpt4(Config config) throws ClassNotFoundException, SQLException, FileNotFoundException {
        // Build the fact tables.
        m_factTableBuilder = new FactTableBuilder(FACT_TABLE, DIMENSION_COUNT);
        m_factTable = m_factTableBuilder.getTable();
        // Add those tables to the program-maintained database catalog, so the query generator
        // can be aware of their existences.
        m_database.addTables(m_factTable);
        m_config = config;
        m_connection = new DbConnection(config);
    }

    public void run() throws ClassNotFoundException, SQLException {
        if (m_config.needToGenerateData()) {
            generateData();
        }
    }

    private void generateData() throws SQLException, ClassNotFoundException {
        printLog("Generating data...");
        CreateTableQuerySet createTableQuerySet = new CreateTableQuerySet();
        createTableQuerySet.setAddDropIfExists(true);

        m_factTable.accept(createTableQuerySet);
        createTableQuerySet.addQuery(m_factTableBuilder.getProjectionDDL());
        m_connection.executeQuerySet(createTableQuerySet);

        m_factTableBuilder.populateData(m_config.getDataSize(), 0, m_cardinalities, m_connection);
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
