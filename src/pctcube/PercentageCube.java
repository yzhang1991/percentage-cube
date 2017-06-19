package pctcube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.database.Table;
import pctcube.database.query.CreateTable;
import pctcube.utils.ArgumentParser;

public final class PercentageCube {

    public interface PercentageCubeVisitor {
        void visit(PercentageCube cube);
    }

    public PercentageCube(Database db) {
        m_database = db;
    }

    public PercentageCube(Database db, String[] args) {
        m_database = db;
        ArgumentParser parser = new ArgumentParser();
        parser.parse(args);

        // Fact table
        m_factTable = db.getTableByName(parser.getArgumentValue("table"));
        if (m_factTable == null) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "table");
        }

        // Dimension list
        List<String> dimensionNames = parser.getArgumentValues("dimensions");
        for (String dimensonName : dimensionNames) {
            Column dimension = m_factTable.getColumnByName(dimensonName);
            if (dimension == null) {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "dimensions");
            }
            m_dimensions.add(dimension);
        }
        if (m_dimensions.size() == 0) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "dimensions");
        }

        // Measure column
        m_measure = m_factTable.getColumnByName(parser.getArgumentValue("measure"));
        if (m_measure == null) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "measure");
        }
    }

    public void setFactTable(Table factTable) {
        m_factTable = factTable;
    }

    public Table getFactTable() {
        return m_factTable;
    }

    public void addDimension(Column c) {
        m_dimensions.add(c);
    }

    public List<Column> getDimensions() {
        return Collections.unmodifiableList(m_dimensions);
    }

    public void setMeasure(Column c) {
        m_measure = c;
    }

    public Column getMeasure() {
        return m_measure;
    }

    public void accept(PercentageCubeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String toString() {
        CreateTable visitor = new CreateTable();
        accept(visitor);
        return visitor.toString();
    }

    private Database m_database;
    private Table m_factTable;
    private List<Column> m_dimensions = new ArrayList<>();
    private Column m_measure;

    private static final Logger m_logger = Logger.getLogger(PercentageCube.class.getName());
}
