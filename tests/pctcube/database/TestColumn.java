package pctcube.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import pctcube.Errors;

public class TestColumn {

    @Test
    public void testFixedLengthColumn() {
        Column fixedLengthColumn = new Column("col0", DataType.INTEGER);
        assertEquals("col0 INTEGER", fixedLengthColumn.toString());
        fixedLengthColumn.setNullable(false);
        assertEquals("col0 INTEGER NOT NULL", fixedLengthColumn.toString());
        try {
            fixedLengthColumn.setSize(25);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.SETTING_SIZE_FOR_FIXED_LENGTH_COLUMN.getMessage(), "col0")));
        }
        try {
            fixedLengthColumn = new Column("col1", DataType.DATE, 15);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.SETTING_SIZE_FOR_FIXED_LENGTH_COLUMN.getMessage(), "col1")));
        }
    }

    @Test
    public void testVariableLengthColumn() {
        Column variableLengthColumn = new Column("str", DataType.VARCHAR);
        assertEquals(Column.DEFAULT_VARLEN_SIZE, variableLengthColumn.getSize());
        assertEquals("str VARCHAR(80)", variableLengthColumn.toString());

        variableLengthColumn.setSize(100);
        assertEquals("str VARCHAR(100)", variableLengthColumn.toString());
        try {
            variableLengthColumn.setSize(0);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.NEGATIVE_COLUMN_LENGTH.getMessage(), "VARCHAR")));
        }
        try {
            variableLengthColumn.setPrecision(10);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.SETTING_PRECISION_FOR_NON_NUMERICAL_COLUMN.getMessage(), "str")));
        }
        try {
            variableLengthColumn.setScale(10);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.SETTING_SCALE_FOR_NON_NUMERICAL_COLUMN.getMessage(), "str")));
        }
        variableLengthColumn = new Column("str2", DataType.VARCHAR, 25);
        assertEquals("str2 VARCHAR(25)", variableLengthColumn.toString());
    }

    @Test
    public void testNumericalColumn() {
        Column numeric = new Column("abc", DataType.DECIMAL);
        assertEquals(Column.DEFAULT_PRECISION, numeric.getPrecision());
        assertEquals(Column.DEFAULT_SCALE, numeric.getScale());
        assertEquals("abc DECIMAL(37,15)", numeric.toString());

        numeric.setPrecision(38);
        numeric.setScale(10);
        assertEquals("abc DECIMAL(38,10)", numeric.toString());

        try {
            numeric.setSize(6);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.SETTING_SIZE_FOR_FIXED_LENGTH_COLUMN.getMessage(), "abc")));
        }
        try {
            numeric.setPrecision(2048);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.INVALID_PRECISION.getMessage(), "abc")));
        }
        try {
            numeric.setScale(39);
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(
                    String.format(Errors.INVALID_SCALE.getMessage(), "abc", numeric.getPrecision())));
        }
    }

    @Test
    public void testColumnNameWithSpace() {
        Column c = new Column("  abc   ", DataType.DATE);
        assertEquals("abc DATE", c.toString());
        c = new Column("a b c", DataType.INTEGER);
        assertEquals("\"a b c\" INTEGER", c.toString());
        c = new Column("a%", DataType.INTEGER);
        assertEquals("\"a%\" INTEGER", c.toString());
    }

}
