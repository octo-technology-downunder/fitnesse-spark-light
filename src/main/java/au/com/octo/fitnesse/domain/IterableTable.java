package au.com.octo.fitnesse.domain;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static au.com.octo.fitnesse.domain.Transformation.getValueFromString;

public interface IterableTable extends Iterable<DataRow> {

    Logger LOGGER = LoggerFactory.getLogger(IterableTable.class);

    List<String> getHeaders();

    default Transformation getTransformation(String columnName, Map<String, String> transformations) {
        if (MapUtils.isNotEmpty(transformations) && transformations.containsKey(columnName)) {
            LOGGER.info("Calculated transformation for column:{} transform: {}", columnName, getValueFromString(transformations.get(columnName)));
            return getValueFromString(transformations.get(columnName));
        } else {
            return Transformation.UNKNOWN;
        }
    }
}
