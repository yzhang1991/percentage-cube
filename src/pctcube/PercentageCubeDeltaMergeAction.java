package pctcube;

import pctcube.PercentageCube.PercentageCubeVisitor;

public class PercentageCubeDeltaMergeAction implements PercentageCubeVisitor {

    @Override
    public void visit(PercentageCube cube) {
//        Database db = cube.getDatabase();
//        String measure = cube.getMeasure().getQuotedColumnName();
//        for (Table table : db.getTables()) {
//            if (table.getTableName().startsWith(PercentageCubeAggregateAction.DELTA)) {
//                Table mergeTarget = db.getTableByName(
//                        table.getTableName().replaceFirst(PercentageCubeAggregateAction.DELTA, ""));
//                if (mergeTarget == null) {
//                    continue;
//                }
//                cube.addQuery(String.format("DROP TABLE IF EXISTS %s;", INTERMEDIATE_TEMP));
//                StringBuilder queryBuilder = new StringBuilder("ALTER TABLE ");
//                queryBuilder.append(mergeTarget.getTableName()).append(" RENAME TO ");
//                queryBuilder.append(INTERMEDIATE_TEMP).append(";");
//                cube.addQuery(queryBuilder.toString());
//                queryBuilder.setLength(0);
//
//                // Merge
//                // The table schema will be [group by columns], cnt, m
//                queryBuilder.append("SELECT ");
//                List<Column> columns = mergeTarget.getColumns();
//                for (int i = 0; i < columns.size() - 2; i++) {
//                    queryBuilder.append("a.").append(columns.get(i).getQuotedColumnName()).append(", ");
//                }
//                // CNT
//                queryBuilder.append("ZEROIFNULL(a.cnt) + ZEROIFNULL(b.cnt) AS cnt, ");
//                // SUM(m)
//                queryBuilder.append("ZEROIFNULL(a.").append(measure);
//                queryBuilder.append(") + ZEROIFNULL(b.").append(measure).append(") AS ");
//                queryBuilder.append(measure).append("\nINTO ").append(mergeTarget.getTableName());
//                queryBuilder.append("\nFROM ").append(table.getTableName());
//                queryBuilder.append(" a FULL OUTER JOIN ").append(INTERMEDIATE_TEMP).append(" b\nON ");
//                if (columns.size() == 2) {
//                    queryBuilder.append("1 = 1");
//                }
//                else {
//                    for (int i = 0; i < columns.size() - 2; i++) {
//                        queryBuilder.append("a.").append(columns.get(i).getQuotedColumnName()).append(" = b.");
//                        queryBuilder.append(columns.get(i).getQuotedColumnName());
//                        if (i != columns.size() - 3) {
//                            queryBuilder.append(" AND ");
//                        }
//                    }
//                }
//                queryBuilder.append(";");
//                cube.addQuery(queryBuilder.toString());
//                queryBuilder.setLength(0);
//
//                cube.addQuery(String.format("DROP TABLE %s;", INTERMEDIATE_TEMP));
//            }
//        }

    }

    private static final String INTERMEDIATE_TEMP = "INTERMEDIATE_TEMP";

}
