package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;

public class DoubleTransformationComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expected, String actual) {
        String expectedValue = getDoubleRepresentation(expected);
        String actualValue = getDoubleRepresentation(actual);
        CellComparisonBuilder cellComparisonBuilder = new CellComparisonBuilder();
        if (compareStringValues(expectedValue, actualValue)) {
            return cellComparisonBuilder.getSuccess(expectedValue);
        } else {
            return cellComparisonBuilder.getFailure(expectedValue, actualValue);
        }
    }

    private String getDoubleRepresentation(String value) {
        return value == null ? null : Double.parseDouble(value) + "";
    }
}
