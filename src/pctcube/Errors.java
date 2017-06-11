package pctcube;

import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

public enum Errors {

    INVALID_PERCENTAGE_CUBE_ARGS (
            IllegalArgumentException.class,
            "Cannot instantiate a percentage cube: invalid '%s' argument"),

    // For ArgumentParser
    INVALID_ARGUMENT_DELIMITER (
            IllegalArgumentException.class,
            "The argument delimiter cannot be null and has to be of length 1"),
    INVALID_VALUE_DELIMITER (
            IllegalArgumentException.class,
            "The value delimiter cannot be null and has to be of length 1"),
    ARGUMENT_NOT_FOUND (
            IllegalArgumentException.class,
            "Argument '%s' is not found"),

    // For database and sql generators
    SETTING_SIZE_FOR_FIXED_LENGTH_COLUMN (
            IllegalArgumentException.class,
            "Cannot set column size for fixed-length type column '%s'"),
    NEGATIVE_COLUMN_LENGTH (
            IllegalArgumentException.class,
            "Length for type %s must be at least 1"),
    SETTING_PRECISION_FOR_NON_NUMERICAL_COLUMN (
            IllegalArgumentException.class,
            "Cannot set precision for non-numerical column '%s'"),
    INVALID_PRECISION (
            IllegalArgumentException.class,
            "Invalid precision for column '%s', the precision must be positive and <= 1024"),
    SETTING_SCALE_FOR_NON_NUMERICAL_COLUMN (
            IllegalArgumentException.class,
            "Cannot set scale for non-numerical column '%s'"),
    INVALID_SCALE (
            IllegalArgumentException.class,
            "Invalid scale for column '%s', the scale must be non-negative and <= scale(%d)"),
    COLUMN_ALREADY_EXISTS (
            IllegalArgumentException.class,
            "Column '%s' already exists"),
    TABLE_ALREADY_EXISTS (
            IllegalArgumentException.class,
            "Table '%s' already exists"),
    PARENT_TABLE_NOT_FOUND (
            RuntimeException.class,
            "Cannot find the table which this column '%s' is associated with");

    Errors(Class<? extends RuntimeException> exceptionClass, String errorMessage) {
        m_exceptionClass = exceptionClass;
        m_message = errorMessage;
    }

    private final Class<? extends RuntimeException> m_exceptionClass;
    private final String m_message;
    private static final Logger m_logger = Logger.getLogger("ErrorConstructor");

    public void throwIt(Logger logger, Object... params) {
        RuntimeException ex;
        String message = m_message;
        if (params != null) {
            message = String.format(message, params);
        }
        try {
            ex = m_exceptionClass.getConstructor(String.class).newInstance(message);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            m_logger.severe(e.toString());
            ex = new RuntimeException(message);
        }
        if (logger != null) {
            logger.severe(ex.toString());
        }
        throw ex;
    }

    public void throwIt()  {
        throwIt(null);
    }

    public String getMessage() {
        return m_message;
    }
}
