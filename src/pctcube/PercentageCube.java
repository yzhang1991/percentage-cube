package pctcube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.database.Table;
import pctcube.database.query.QuerySet;

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
        if (m_evaluationMethod == EvaluationMethod.GROUPBY) {
            accept(new PercentageCubeAggregateAction());
        }
        accept(new PercentageCubeAssembler());
        accept(new PercentageCubeTopKFilter());
    }

    public void evaluateIncrementallyOn(Table deltaFactTable) {
        for (Column dimension : m_dimensions) {
            if (! deltaFactTable.getColumnByName(dimension.getColumnName()).equals(dimension)) {
                throw new IllegalArgumentException("Invalid delta fact table.");
            }
        }

        clear();
        accept(new PercentageCubeCreateAction());
        if (m_evaluationMethod == EvaluationMethod.GROUPBY) {
            accept(new PercentageCubeAggregateAction(deltaFactTable));
            accept(new PercentageCubeDeltaMergeAction());
        }
        accept(new PercentageCubeAssembler());
        accept(new PercentageCubeTopKFilter());
    }

    public PercentageCube(Database db, String[] args) {
        accept(new PercentageCubeInitializer(db, args));
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

    public int getRowCountThreshold() {
        return m_rowCount;
    }

    public boolean isIncremental() {
        return m_incremental;
    }

    public Database getDatabase() {
        return m_database;
    }

    public Table getPercentageCubeTable() {
        return m_pctCubeTable;
    }

    public Table getOLAPCubeTable() {
        return m_olapCubeTable;
    }

    public boolean usesUDF() {
        return m_useUDF;
    }

    protected Database m_database;
    protected Table m_factTable;
    protected Table m_pctCubeTable;
    protected Table m_olapCubeTable;

    protected List<Column> m_dimensions = new ArrayList<>();
    protected Column m_measure;
    protected PruningStrategy m_pruningStrategy = PruningStrategy.NONE;
    protected EvaluationMethod m_evaluationMethod = EvaluationMethod.GROUPBY;
    protected int m_topk = 0;
    protected int m_rowCount = 0; // row count, zero means no threshold applied.
    protected boolean m_incremental = false;
    protected boolean m_useUDF = false; // whether use the user-defined aggregate function sumnull()
    // sumnull() will return null if any of the values being summed is null.

    protected static final Logger m_logger = Logger.getLogger(PercentageCube.class.getName());
}
