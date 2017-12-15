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

    private static final int DIMENSION_COUNT = 8;
    private static final String FACT_TABLE = "fact";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, FileNotFoundException {
        Config config = Config.getConfigFromFile("config.ini");
        jPctCubeExpt4 experiment = new jPctCubeExpt4(config);
        experiment.run();
    }

    private int[] m_cardinalities = new int[] {100, 1000, 10000, 100000, 1000000, 10000000, 10, 100};
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

        StringBuffer result = new StringBuffer();
        result.append(String.format("%20s%10s%10s\n", "cardinality", "GROUP-BY", "OLAP"));
        result.append(String.format("%10s%10s\n", "|L|", "|R|"));
        // break down by key
        for (int j = 6; j < 8; j++) {
            // total by key
            for (int i = 0; i < 6; i++) {
                m_connection.execute("DROP TABLE IF EXISTS Findv;");
                m_connection.execute("DROP TABLE IF EXISTS Ftotal;");
                m_connection.execute("DROP TABLE IF EXISTS pct;");

                // Get Findv
                StringBuffer queryBuilder = new StringBuffer("SELECT ");
                queryBuilder.append("d").append(i).append(", d").append(j).append(", SUM(m) AS m INTO Findv FROM ");
                queryBuilder.append(FACT_TABLE).append(" GROUP BY d").append(i).append(", d").append(j).append(";\n");

                // Get Ftotal
                queryBuilder.append("SELECT d").append(i).append(", SUM(m) AS m INTO Ftotal FROM Findv ");
                queryBuilder.append(" GROUP BY d").append(i).append(";\n");

                // Join
                queryBuilder.append("SELECT Findv.d").append(i).append(", Findv.d").append(j);
                queryBuilder.append(", (CASE WHEN Ftotal.m <> 0 THEN Findv.m/Ftotal.m ELSE NULL END) AS pct ");
                queryBuilder.append("FROM Ftotal JOIN Findv ON Ftotal.d").append(i).append(" = Findv.d").append(i).append(";");

                printLog("Start percentage aggregation using group-by, total by d%d, break down by d%d.", i, j);
                long startTime = System.currentTimeMillis();
                m_connection.execute(queryBuilder.toString());
                long endTime = System.currentTimeMillis();
                double durationGroupBy = (endTime - startTime) / 1000.0;
                printLog("Finished in %.2f seconds.", durationGroupBy);
                m_connection.execute("SELECT CLEAR_CACHES();");

                // OLAP
                queryBuilder = new StringBuffer("SELECT ");
                queryBuilder.append("d").append(i).append(", d").append(j).append(", (CASE WHEN Y <> 0 THEN X/Y ELSE NULL END) AS pct FROM ");
                queryBuilder.append("(SELECT d").append(i).append(", d").append(j).append(", SUM(m) OVER (PARTITION BY d").append(i);
                queryBuilder.append(", d").append(j).append(") AS X, SUM(m) OVER (PARTITION BY d").append(i).append(") AS Y, ");
                queryBuilder.append("row_number() OVER (PARTITION BY d").append(i).append(", d").append(j).append(") AS rnumber FROM ");
                queryBuilder.append(FACT_TABLE).append(") foo WHERE rnumber = 1;");

                printLog("Start percentage aggregation using OLAP, total by d%d, break down by d%d.", i, j);
                startTime = System.currentTimeMillis();
                m_connection.execute(queryBuilder.toString());
                endTime = System.currentTimeMillis();
                double durationOLAP = (endTime - startTime) / 1000.0;
                printLog("Finished in %.2f seconds.", durationOLAP);
                m_connection.execute("SELECT CLEAR_CACHES();");
                result.append(String.format("%10d%10d%10.2f%10.2f\n", m_cardinalities[i], m_cardinalities[j], durationGroupBy, durationOLAP));
            }

        }
        printLog(result.toString());
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
