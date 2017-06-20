package pctcube;

import java.util.ArrayList;
import java.util.List;

import pctcube.database.Table;

public class AggregationTempTable extends Table {

    public AggregationTempTable(List<Integer> selectionFlags) {
        m_selectionFlags = new ArrayList<>(selectionFlags);
        StringBuilder builder = new StringBuilder(TEMP_TABLE_PREFIX);
        for (int i = 0; i < m_selectionFlags.size(); i++) {
            if (m_selectionFlags.get(i) > 0) {
                builder.append(i).append("_");
            }
        }
        builder.setLength(builder.length() - 1);
        setTableName(builder.toString());
        setTempTable(true);
    }

    public void setSelectionFlags(List<Integer> flags) {
        m_selectionFlags = new ArrayList<>(flags);
    }

    public boolean canDeriveFrom(AggregationTempTable otherTable) {
        if (m_selectionFlags == null || otherTable.m_selectionFlags == null) {
            return false;
        }
        if (m_selectionFlags.size() != otherTable.m_selectionFlags.size()) {
            return false;
        }
        for (int i = 0; i < m_selectionFlags.size(); i++) {
            if (m_selectionFlags.get(i) > 0 && otherTable.m_selectionFlags.get(i) == 0) {
                return false;
            }
        }
        return true;
    }

    private List<Integer> m_selectionFlags;

    private static final String TEMP_TABLE_PREFIX = "TEMP_AGG_";
}
