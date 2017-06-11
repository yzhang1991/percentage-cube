package pctcube.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import pctcube.Errors;

public class TestArgumentParser {

    @Test
    public void testArgument() {
        String argumentName = "arg0";
        Argument argument = new Argument(argumentName);
        assertEquals(argumentName, argument.getName());
        List<String> argumentValues = argument.getValues();
        assertEquals(0, argumentValues.size());
        assertEquals("ARGUMENT [name = \"arg0\" values = {<NO VALUE>}]", argument.toString());

        // Add five values
        for (int i = 0; i < 4; i++) {
            argument.addValue("value" + String.valueOf(i));
        }

        argumentValues = argument.getValues();
        assertEquals(4, argumentValues.size());
        for (int i = 0; i < 4; i++) {
            assertEquals("value" + String.valueOf(i), argumentValues.get(i));
        }
        assertEquals(
                "ARGUMENT [name = \"arg0\" values = {\"value0\", \"value1\", \"value2\", \"value3\"}]",
                argument.toString());
    }

    private void verifyParsingResult(ArgumentParser parser, String[] args) {
        Map<String, Argument> parsedArguments = parser.parse(args);
        assertEquals(3, parsedArguments.size());
        assertTrue(parsedArguments.get("arg0").equals(expectedArg0));
        assertTrue(parsedArguments.get("arg1").equals(expectedArg1));
        assertTrue(parsedArguments.get("arg2").equals(expectedArg2));
    }

    @Test
    public void testArgumentParser() {
        ArgumentParser parser = new ArgumentParser();
        // Test <null> input.
        Map<String, Argument> parsedArguments = parser.parse(null);
        assertEquals(0, parsedArguments.size());
        // Test empty input.
        parsedArguments = parser.parse(new String[] {});
        assertEquals(0, parsedArguments.size());

        verifyParsingResult(parser,
                new String[] {"arg0=value0;  arg1=value1;arg2=value2, value3 , value4;"});
        verifyParsingResult(parser,
                new String[] {"arg0=value0;", "arg1=value1", "arg2=value2, value3 , value4"});
        // Test argument showed up for more than once. The values should be appended.
        verifyParsingResult(parser,
                new String[] {"arg0=value0;", "arg2=value2", "arg1=value1", "arg2=value3 , value4"});
        // Test different delimiters.
        parser.setArgumentDelimiter(":");
        parser.setValueDelimiter(">");
        verifyParsingResult(parser,
                new String[] {"arg0=value0:", "arg1   =value1:arg2=value2> value3 > value4"});
    }

    @Test
    public void testArgumentParserExceptions() {
        ArgumentParser parser = new ArgumentParser();
        try {
            parser.setArgumentDelimiter("invalid");
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(Errors.INVALID_ARGUMENT_DELIMITER.getMessage()));
        }
        try {
            parser.setValueDelimiter("invalid");
        }
        catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(Errors.INVALID_VALUE_DELIMITER.getMessage()));
        }
    }

    static final Argument expectedArg0 = new Argument("arg0");
    static final Argument expectedArg1 = new Argument("arg1");
    static final Argument expectedArg2 = new Argument("arg2");

    static {
        expectedArg0.addValue("value0");
        expectedArg1.addValue("value1");
        expectedArg2.addValue("value2");
        expectedArg2.addValue("value3");
        expectedArg2.addValue("value4");
    }

}
