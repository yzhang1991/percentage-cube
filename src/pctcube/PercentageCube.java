package pctcube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.database.Table;
import pctcube.database.query.QuerySet;
import pctcube.utils.ArgumentParser;

public final class PercentageCube extends QuerySet {

    public interface PercentageCubeVisitor {
        void visit(PercentageCube cube);
    }

    public void accept(PercentageCubeVisitor visitor) {
        visitor.visit(this);
    }

    public void evaluate() {
        clear();
        accept(new PercentageCubeCreateAction());
        accept(new PercentageCubeAggregateAction());
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
        if (dimensionNames == null) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "dimensions");
        }
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

        // Method
        String method = parser.getArgumentValue("method");
        if (method != null) {
            if (method.equals("groupby")) {
                m_evaluationMethod = EvaluationMethod.GROUPBY;
            }
            else if (method.equals("olap")) {
                m_evaluationMethod = EvaluationMethod.OLAP;
            }
            else {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "method");
            }
        }

        // pruning
        String pruning = parser.getArgumentValue("pruning");
        if (pruning != null) {
            if (pruning.equals("none")) {
                m_pruningStrategy = PruningStrategy.NONE;
            }
            else if (pruning.equals("direct")) {
                m_pruningStrategy = PruningStrategy.DIRECT;
            }
            else if (pruning.equals("cascade")) {
                m_pruningStrategy = PruningStrategy.CASCADE;
            }
            else {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "pruning");
            }
        }

        // topk
        String topk = parser.getArgumentValue("topk");
        if (topk != null) {
            try {
                m_topk = Integer.valueOf(topk);
            }
            catch (NumberFormatException e) {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(m_logger, "topk");
            }
        }

        // incremental
        m_incremental = Boolean.valueOf(parser.getArgumentValue("incremental"));
    }

    public Table getFactTable() {
        return m_factTable;
    }

    public List<Column> getDimensions() {
        return Collections.unmodifiableList(m_dimensions);
    }

    public Column getMeasure() {
        return m_measure;
    }

    public PruningStrategy getPruningStrategy() {
        return m_pruningStrategy;
    }

    public EvaluationMethod getEvaluationMethod() {
        return m_evaluationMethod;
    }

    // zero means disabled.
    public int getTopK() {
        return m_topk;
    }

    public boolean isIncremental() {
        return m_incremental;
    }

    public Database getDatabase() {
        return m_database;
    }

    private Database m_database;
    private Table m_factTable;
    private List<Column> m_dimensions = new ArrayList<>();
    private Column m_measure;
    private PruningStrategy m_pruningStrategy = PruningStrategy.NONE;
    private EvaluationMethod m_evaluationMethod = EvaluationMethod.GROUPBY;
    private int m_topk = 0;
    private boolean m_incremental = false;

    private static final Logger m_logger = Logger.getLogger(PercentageCube.class.getName());
}
