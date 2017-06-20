package pctcube.database.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class QuerySet {

    public QuerySet() {
        setIndentation(4);
    }

    public List<String> getQueries() {
        return Collections.unmodifiableList(m_queries);
    }

    public void setIndentation(int size) {
        m_indentation = String.join("", Collections.nCopies(size, " "));
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

    public String getIndentationString(int indentLevel) {
        return String.join("", Collections.nCopies(indentLevel, m_indentation));
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

    private String m_indentation;
    private List<String> m_queries = new ArrayList<>();
}
