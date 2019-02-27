package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;

public class TrimTransformationComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expectedValue, String actualValue) {
        String expectedTrimmedValue = getTrimmedValue(expectedValue);
        String actualTrimmedValue = getTrimmedValue(actualValue);
        CellComparisonBuilder helper = new CellComparisonBuilder();
        if (compareStringValues(expectedTrimmedValue, actualTrimmedValue)) {
            return helper.getSuccess(expectedTrimmedValue);
        } else {
            return helper.getFailure(expectedTrimmedValue, actualTrimmedValue);
        }
    }

    private String getTrimmedValue(String value) {
        return value == null ? null : value.trim();
    }
}
