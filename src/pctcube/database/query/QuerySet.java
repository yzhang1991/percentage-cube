package pctcube.database.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class QuerySet {

    private static final int INDENTATION_SIZE = 4;

    private List<String> m_queries = new ArrayList<>();

    public static String getIndentationString(int level) {
        return String.join("", Collections.nCopies(level * INDENTATION_SIZE, " "));
    }

    public List<String> getQueries() {
        return Collections.unmodifiableList(m_queries);
    }

    public void clear() {
        m_queries.clear();
    }

    public void addQuery(String query) {
        m_queries.add(query);
    }

    public void addAllQueries(List<String> queries) {
        m_queries.addAll(queries);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (String query : m_queries) {
            builder.append(query).append("\n\n");
        }
        if (m_queries.size() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }
}
