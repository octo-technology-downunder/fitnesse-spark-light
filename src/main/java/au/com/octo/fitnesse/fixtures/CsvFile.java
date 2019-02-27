package au.com.octo.fitnesse.fixtures;

import au.com.octo.fitnesse.domain.IteratorListDataTable;
import au.com.octo.fitnesse.fixtures.utils.CompareUtil;
import au.com.octo.fitnesse.fixtures.utils.CsvUtil;
import au.com.octo.fitnesse.fixtures.utils.SlimMessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CsvFile {

    private final String table;
    private final Map<String, String> readColumnTransformations = new HashMap<>();
    private final static Logger LOGGER = LoggerFactory.getLogger(CsvFile.class);

    public CsvFile(String table) {
        this.table = table;
    }

    public CsvFile(String table, String rawReadColumnTransformations) {
        this.table = table;
        this.readColumnTransformations.putAll(extractColumnTransformations(rawReadColumnTransformations));
    }

    public Map<String, String> extractColumnTransformations(String rawColumnTransformations) {
        LOGGER.info("Applying transformations: " + rawColumnTransformations);
        Map<String, String> readColumnTransformations = new HashMap<>();
        if (rawColumnTransformations != null) {
            for (String part : rawColumnTransformations.split(",")) {
                if (!part.contains(":")) {
                    throw new RuntimeException("Bad ColumnTransformations Format: expecting 'COL_NAME1:TRANSFORMATION_NAME1,COL_NAME2:TRANSFORMATION_NAME2', got " + rawColumnTransformations);
                }
                String[] item = part.split(":");
                if (item.length != 2) {
                    throw new RuntimeException(
                            "Bad ColumnTransformations Format, expecting 2 parts: expecting 'COL_NAME1:TRANSFORMATION_NAME1,COL_NAME2:TRANSFORMATION_NAME2', got " + rawColumnTransformations);
                }
                readColumnTransformations.put(item[0].trim(), item[1].trim());
            }
        }
        return readColumnTransformations;
    }

    public List<List<String>> doTable(List<List<String>> expectedRows) throws IOException {
        LOGGER.info("ExpectedRows from: " + table);
        if (table.startsWith(">>")) {
            String file = table.replaceAll(">>", "").trim();
            CsvUtil.write(file, expectedRows.iterator());
            return SlimMessageUtils.passTable("Result saved to file " + file, expectedRows);
        } else {
            Iterator<List<String>> actualRows = CsvUtil.read(table);
            return CompareUtil.compareTable(expectedRows, new IteratorListDataTable(actualRows), readColumnTransformations);
        }
    }

}
