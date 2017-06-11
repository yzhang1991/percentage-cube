package pctcube.utils;

import java.util.ArrayList;
import java.util.List;

public final class Argument {

    public Argument(final String name) {
        m_name = name;
    }

    public void addValue(final String value) {
        m_values.add(value.trim());
    }

    public void addValues(final String[] values) {
        for (String value : values) {
            addValue(value);
        }
    }

    public List<String> getValues() {
        return new ArrayList<String>(m_values);
    }

    public String getValue(int index) {
        return m_values.get(index);
    }

    public String getName() {
        return m_name;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ARGUMENT [name = \"");
        sb.append(m_name).append("\" values = {");
        if (m_values.size() == 0) {
            sb.append("<NO VALUE>  ");
        }
        else {
            for (String value : m_values) {
                sb.append("\"").append(value).append("\", ");
            }
        }
        sb.setLength(sb.length() - 2);
        sb.append("}]");
        return sb.toString();
    }

    public boolean equals(Argument other) {
        if (! m_name.equals(other.getName())) {
            return false;
        }
        List<String> theOtherValues = other.getValues();
        if (m_values.size() != theOtherValues.size()) {
            return false;
        }
        for (int i = 0; i < m_values.size(); i++) {
            if (! m_values.get(i).equals(theOtherValues.get(i))) {
                return false;
            }
        }
        return true;
    }

    private final String m_name;
    private final List<String> m_values = new ArrayList<>();

}
