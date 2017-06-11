package pctcube.database;

public enum DataType {

    // Only has the ones used in the TPC-H schema
    INTEGER("INTEGER"),
    FLOAT("FLOAT"),
    CHAR("CHAR", true, false),
    VARCHAR("VARCHAR", true, false),
    DECIMAL("DECIMAL", false, true),
    DATE("DATE");

    DataType(String name) {
        m_name = name;
        m_lengthVariable = false;
        m_hasPrecisionAndScale = false;
    }

    DataType(String name, boolean lengthVariable, boolean hasPrecisionAndScale) {
        m_name = name;
        m_lengthVariable = lengthVariable;
        m_hasPrecisionAndScale = hasPrecisionAndScale;
    }

    public String getTypeName() {
        return m_name;
    }

    public boolean isVariableLengthType() {
        return m_lengthVariable;
    }

    public boolean hasPrecisionAndScale() {
        return m_hasPrecisionAndScale;
    }

    private final String m_name;
    private final boolean m_lengthVariable;
    private final boolean m_hasPrecisionAndScale;
}
