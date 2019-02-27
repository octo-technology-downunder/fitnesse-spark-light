package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonBuilder;
import au.com.octo.fitnesse.domain.CellComparisonResult;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import static au.com.octo.fitnesse.fixtures.utils.Constants.BINARY_DATA;
import static au.com.octo.fitnesse.fixtures.utils.Constants.OUTPUT;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class BlobComparator implements ValueComparator<String, String> {

    @Override
    public CellComparisonResult compare(String expectedValue, String actualValue) {
        CellComparisonBuilder helper = new CellComparisonBuilder();
        try {
            byte[] actualData = java.nio.file.Files.readAllBytes(Paths.get(OUTPUT + actualValue));
            boolean isEqual = Arrays.equals(actualData, expectedValue.getBytes(ISO_8859_1));
            if (isEqual) {
                return helper.getSuccess(actualValue);
            } else {
                return helper.getFailure(BINARY_DATA, actualValue);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
