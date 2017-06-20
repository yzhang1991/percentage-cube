package pctcube;

import java.util.List;

import pctcube.PercentageCube.PercentageCubeVisitor;
import pctcube.database.Column;
import pctcube.database.Database;
import pctcube.utils.ArgumentParser;

public class PercentageCubeInitializer implements PercentageCubeVisitor {

    public PercentageCubeInitializer(Database database, String[] args) {
        m_database = database;
        m_args = args;
    }


    @Override
    public void visit(PercentageCube cube) {
        cube.m_database = m_database;
        ArgumentParser parser = new ArgumentParser();
        parser.parse(m_args);

        // Fact table
        cube.m_factTable = m_database.getTableByName(parser.getArgumentValue("table"));
        if (cube.m_factTable == null) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "table");
        }

        // Dimension list
        List<String> dimensionNames = parser.getArgumentValues("dimensions");
        if (dimensionNames == null) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "dimensions");
        }
        for (String dimensonName : dimensionNames) {
            Column dimension = cube.m_factTable.getColumnByName(dimensonName);
            if (dimension == null) {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "dimensions");
            }
            cube.m_dimensions.add(dimension);
        }
        if (cube.m_dimensions.size() == 0) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "dimensions");
        }

        // Measure column
        cube.m_measure = cube.m_factTable.getColumnByName(parser.getArgumentValue("measure"));
        if (cube.m_measure == null) {
            Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "measure");
        }

        // Method
        String method = parser.getArgumentValue("method");
        if (method != null) {
            if (method.equals("groupby")) {
                cube.m_evaluationMethod = EvaluationMethod.GROUPBY;
            }
            else if (method.equals("olap")) {
                cube.m_evaluationMethod = EvaluationMethod.OLAP;
            }
            else {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "method");
            }
        }

        // pruning
        String pruning = parser.getArgumentValue("pruning");
        if (pruning != null) {
            if (pruning.equals("none")) {
                cube.m_pruningStrategy = PruningStrategy.NONE;
            }
            else if (pruning.equals("direct")) {
                cube.m_pruningStrategy = PruningStrategy.DIRECT;
            }
            else if (pruning.equals("cascade")) {
                cube.m_pruningStrategy = PruningStrategy.CASCADE;
            }
            else {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "pruning");
            }
        }

        // topk
        String topk = parser.getArgumentValue("topk");
        if (topk != null) {
            try {
                cube.m_topk = Integer.valueOf(topk);
            }
            catch (NumberFormatException e) {
                Errors.INVALID_PERCENTAGE_CUBE_ARGS.throwIt(PercentageCube.m_logger, "topk");
            }
        }

        // incremental
        cube.m_incremental = Boolean.valueOf(parser.getArgumentValue("incremental"));
    }

    private final Database m_database;
    private final String[] m_args;
}
