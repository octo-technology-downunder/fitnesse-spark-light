package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;

public class HeaderValueComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expectedHeader, String actualHeader) {
        CellComparisonBuilder cellComparisonBuilder = new CellComparisonBuilder();
        if (equals(expectedHeader, actualHeader))
            return cellComparisonBuilder.getSuccess(expectedHeader);
        else
            return cellComparisonBuilder.getFailure(expectedHeader, actualHeader);
    }

    private static boolean equals(String expected, String actual) {
        if (expected == null) {
            return actual == null;
        } else {
            return expected.equalsIgnoreCase(actual);
        }
    }
}
