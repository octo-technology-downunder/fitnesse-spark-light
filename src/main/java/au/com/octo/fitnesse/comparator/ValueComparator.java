package au.com.octo.fitnesse.comparator;

import au.com.octo.fitnesse.domain.CellComparisonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface ValueComparator<V1, V2> {

    Logger LOGGER = LoggerFactory.getLogger(ValueComparator.class);

    CellComparisonResult compare(V1 value1, V2 value2);

    default boolean compareStringValues(String expected, String actual) {
        if (expected == null) {
            return actual == null;
        } else {
            return expected.equals(actual);
        }
    }

}
