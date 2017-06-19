package pctcube.database;

import java.util.logging.Logger;
import java.util.regex.Pattern;

import pctcube.Errors;

public final class Column {

    public Column(String name, DataType dataType) {
        m_name = name.trim();
        m_dataType = dataType;
        if (m_dataType.hasPrecisionAndScale()) {
            m_precision = DEFAULT_PRECISION;
            m_scale = DEFAULT_SCALE;
        }
        else if (m_dataType.isVariableLengthType()) {
            m_size = DEFAULT_VARLEN_SIZE;
        }
    }

    public Column(Column copyFrom) {
        m_name = copyFrom.m_name;
        m_dataType = copyFrom.m_dataType;
        m_precision = copyFrom.m_precision;
        m_scale = copyFrom.m_scale;
        m_size = copyFrom.m_size;
        m_nullable = copyFrom.m_nullable;
        m_tableBelongedTo = copyFrom.m_tableBelongedTo;
    }

    public Column(String name, DataType columnType, int size) {
        m_name = name;
        m_dataType = columnType;
        setSize(size);
    }

    public Column(String name, DataType columnType, int precision, int scale) {
        m_name = name;
        m_dataType = columnType;
        setPrecision(precision);
        setScale(scale);
    }

    public String getColumnName() {
        return m_name;
    }

    public void setColumnName(String newName) {
        m_name = newName;
    }

    public DataType getDataType() {
        return m_dataType;
    }

    public int getSize() {
        return m_size;
    }

    public int getPrecision() {
        return m_precision;
    }

    public int getScale() {
        return m_scale;
    }

    public boolean isNullable() {
        return m_nullable;
    }

    public Column setNullable(boolean value) {
        m_nullable = value;
        return this;
    }

    public void setSize(int size) {
        if (m_dataType.isVariableLengthType()) {
            if (size < 1) {
                Errors.NEGATIVE_COLUMN_LENGTH.throwIt(m_logger, m_dataType.getTypeName());
            }
            m_size = size;
        }
        else {
            Errors.SETTING_SIZE_FOR_FIXED_LENGTH_COLUMN.throwIt(m_logger, m_name);
        }
    }

    public void setPrecision(int precision) {
        if (m_dataType.hasPrecisionAndScale()) {
            if (precision <= 0 || precision > 1024) {
                Errors.INVALID_PRECISION.throwIt(m_logger, m_name);
            }
            m_precision = precision;
        }
        else {
            Errors.SETTING_PRECISION_FOR_NON_NUMERICAL_COLUMN.throwIt(m_logger, m_name);
        }
    }

    public void setScale(int scale) {
        if (m_dataType.hasPrecisionAndScale()) {
            if (scale < 0 || scale > m_precision) {
                Errors.INVALID_SCALE.throwIt(m_logger, m_name, m_precision);
            }
            m_scale = scale;
        }
        else {
            Errors.SETTING_SCALE_FOR_NON_NUMERICAL_COLUMN.throwIt(m_logger, m_name);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getQuotedColumnName()).append(" ");
        builder.append(getDataType().getTypeName());
        // For variable-length column, append the column size after the column type.
        if (getDataType().hasPrecisionAndScale()) {
            builder.append("(").append(getPrecision());
            builder.append(",").append(getScale()).append(")");
        }
        else if (getDataType().isVariableLengthType()) {
            builder.append("(").append(getSize()).append(")");
        }
        if (! isNullable()) {
            builder.append(" NOT NULL");
        }
        return builder.toString();
    }

    // Only Table.addColumn() should be able to perform this action.
    protected void associateWithTable(Table table) {
        m_tableBelongedTo = table;
    }

    public Table getTableThisBelongsTo() {
        return m_tableBelongedTo;
    }

    public String getQuotedColumnName() {
        // If column name has space or some other symbols, enclose it with double quotation mark.
        if (PAT_SHOULD_ADD_QUOTATION_MARK.matcher(m_name).matches()) {
            return "\"" + m_name + "\"";
        }
        return m_name;
    }

    private String m_name;
    private DataType m_dataType;
    private int m_size = -1;
    private int m_precision = -1;
    private int m_scale = -1;
    private boolean m_nullable = true;
    private Table m_tableBelongedTo;

    // For Vertica
    public static final int DEFAULT_VARLEN_SIZE = 80;
    public static final int DEFAULT_PRECISION = 37;
    public static final int DEFAULT_SCALE = 15;

    private static final Logger m_logger = Logger.getLogger(Column.class.getName());
    private static final Pattern PAT_SHOULD_ADD_QUOTATION_MARK = Pattern.compile(".*[^a-zA-Z0-9_].*");
}
