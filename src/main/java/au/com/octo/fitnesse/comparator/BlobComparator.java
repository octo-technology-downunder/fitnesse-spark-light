package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;
import au.com.octo.fitnesse.fixtures.utils.Constants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;

import static au.com.octo.fitnesse.fixtures.utils.Constants.OUTPUT;

public class BlobComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expectedValue, String actualValue) {
        CellComparisonBuilder helper = new CellComparisonBuilder();
        try {
            byte[] actualData = java.nio.file.Files.readAllBytes(Paths.get(OUTPUT + actualValue));
            boolean isEqual = Arrays.equals(actualData, expectedValue.getBytes(StandardCharsets.ISO_8859_1));
            if (isEqual) {
                return helper.getSuccess(actualValue.toString());
            } else {
                return helper.getFailure(Constants.BINARY_DATA, actualValue.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
