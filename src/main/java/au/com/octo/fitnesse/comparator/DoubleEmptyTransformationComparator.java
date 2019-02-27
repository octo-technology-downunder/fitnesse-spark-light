package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;

public class DoubleEmptyTransformationComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expected, String actual) {
        String expectedValue = getDoubleEmptyRepresentation(expected);
        String actualValue = getDoubleEmptyRepresentation(actual);
        CellComparisonBuilder cellComparisonBuilder = new CellComparisonBuilder();
        if (compareStringValues(expectedValue, actualValue)) {
            return cellComparisonBuilder.getSuccess(expectedValue);
        } else {
            return cellComparisonBuilder.getFailure(expectedValue, actualValue);
        }
    }

    private String getDoubleEmptyRepresentation(String value) {
        return value == null || "".equals(value) ? "" : Double.parseDouble(value) + "";
    }
}
