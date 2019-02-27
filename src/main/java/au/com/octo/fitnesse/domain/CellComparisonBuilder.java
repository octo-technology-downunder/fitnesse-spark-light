package au.com.octo.fitnesse.domain;

import au.com.octo.fitnesse.fixtures.utils.SlimMessageUtils;

public class CellComparisonBuilder {

    public CellComparisonResult getSuccess(String expectedValue) {
        return prepareSuccessMessage(expectedValue);
    }

    public CellComparisonResult getFailure(String expectedValue, String actualValue) {
        return prepareFailureMessage(expectedValue, actualValue);
    }

    private CellComparisonResult prepareFailureMessage(String expectedValue, String actualValue) {
        String message = SlimMessageUtils.fail(expectedValue, actualValue);
        return new CellComparisonResult(true, message);
    }

    private CellComparisonResult prepareSuccessMessage(String expectedValue) {
        String message = SlimMessageUtils.pass(expectedValue);
        return new CellComparisonResult(false, message);
    }
}
