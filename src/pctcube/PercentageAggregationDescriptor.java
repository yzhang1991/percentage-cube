package pctcube;

import pctcube.PercentageAggregation.PercentageAggregationVisitor;
import pctcube.database.Column;

public class PercentageAggregationDescriptor implements PercentageAggregationVisitor {

    public PercentageAggregationDescriptor(StringBuilder builder) {
        m_builder = builder;
    }

    @Override
    public void visit(PercentageAggregation agg) {
        m_builder.append("Percentage aggregation on fact table '");
        m_builder.append(agg.getFactTable().getTableName()).append("'\n");
        m_builder.append("MEASURE: ").append(agg.getMeasure().getQuotedColumnName());
        m_builder.append("\nTOTAL BY: ");
        for (Column c : agg.getTotalByKeys()) {
            m_builder.append(c.getQuotedColumnName()).append(",");
        }
        m_builder.setLength(m_builder.length() - 1);
        m_builder.append("\nBREAK DOWN BY: ");
        for (Column c : agg.getBreakdownByKeys()) {
            m_builder.append(c.getQuotedColumnName()).append(",");
        }
        m_builder.setLength(m_builder.length() - 1);
    }

    private StringBuilder m_builder;
}
