package pctcube.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pctcube.Errors;

public final class ArgumentParser {

    public Map<String, Argument> parse(String[] args) {
        m_parsedArguments = new HashMap<>();
        if (args == null) {
            return m_parsedArguments;
        }

        // Connect all arguments together, add the argument delimiter in between if needed.
        StringBuilder concatArgBuilder = new StringBuilder();
        for(String arg : args) {
            arg = arg.trim();
            concatArgBuilder.append(arg);
            if (arg.length() > 0 && ! arg.endsWith(m_argDelimiter)) {
                concatArgBuilder.append(m_argDelimiter);
            }
        }

        // Split the concatenated argument string using the argument delimiter.
        // Allow the delimiter to show up in the argument if it is escaped by a backslash.
        String[] splittedArgs = concatArgBuilder.toString().split(m_argDelimiter);
        // Split the argument name from the argument value(s).

        for (String arg : splittedArgs) {
            Matcher matcher = PAT_ARGUMENT.matcher(arg.trim());
            if (matcher.matches()) {
                String name = matcher.group(1).trim();
                // If the argument name already showed up before, append the new values to the same entry.
                // Add a warning message to the log.
                Argument argument = m_parsedArguments.get(name);
                if (argument == null) {
                    argument = new Argument(name);
                    m_parsedArguments.put(name, argument);
                }
                else {
                    m_logger.log(Level.WARNING,
                            "Argument {0} showed up for more than once, new argument values are appended.",
                            name);
                }
                String values = matcher.group(2);
                argument.addValues(values.split(m_valueDelimiter));
            }
        }
        return m_parsedArguments;
    }

    public void setArgumentDelimiter(final String argDelimiter) {
        if (argDelimiter == null || argDelimiter.length() != 1) {
            Errors.INVALID_ARGUMENT_DELIMITER.throwIt(m_logger);
        }
        m_argDelimiter = argDelimiter;
    }

    public String getArgumentDelimiter() {
        return m_argDelimiter;
    }

    public void setValueDelimiter(final String valueDelimiter) {
        if (valueDelimiter == null || valueDelimiter.length() != 1) {
            Errors.INVALID_VALUE_DELIMITER.throwIt(m_logger);
        }
        m_valueDelimiter = valueDelimiter;
    }

    public String getValueDelimiter() {
        return m_valueDelimiter;
    }

    public Argument getArgument(String argumentName) {
        Argument arg = m_parsedArguments.get(argumentName);
        if (arg == null) {
            Errors.ARGUMENT_NOT_FOUND.throwIt(m_logger, argumentName);
        }
        return arg;
    }

    public List<String> getArgumentValues(String argumentName) {
        return getArgument(argumentName).getValues();
    }

    public String getArgumentValue(String argumentName) {
        return getArgument(argumentName).getValue(0);
    }

    private String m_argDelimiter = ";";
    private String m_valueDelimiter = ",";
    private Map<String, Argument> m_parsedArguments = new HashMap<>();

    private static final Logger m_logger = Logger.getLogger(ArgumentParser.class.getName());
    private static final Pattern PAT_ARGUMENT = Pattern.compile("^(.+)=(.*)$");
}
