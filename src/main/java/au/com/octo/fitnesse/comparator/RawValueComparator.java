package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;

public class RawValueComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expected, String actual) {
        CellComparisonBuilder helper = new CellComparisonBuilder();
        if (compareStringValues(expected, actual)) {
            return helper.getSuccess(expected);
        } else {
            return helper.getFailure(expected, actual);
        }
    }
}
