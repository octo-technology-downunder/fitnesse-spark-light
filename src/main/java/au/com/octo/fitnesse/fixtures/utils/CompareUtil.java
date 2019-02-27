package au.com.octo.fitnesse.fixtures.utils;

import au.com.octo.fitnesse.comparator.ComparatorFactory;
import au.com.octo.fitnesse.comparator.HeaderValueComparator;
import au.com.octo.fitnesse.comparator.ValueComparator;
import au.com.octo.fitnesse.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompareUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompareUtil.class);

    public static List<List<String>> compareTable(List<List<String>> expectedRows, IterableTable actualRows, Map<String, String> columnTransformations) {
        try {
            if (expectedRows.size() == 1) {
                List<String> firstRow = expectedRows.get(0);
                if (firstRow.size() == 1) {
                    String value = firstRow.get(0).trim();
                    if (value.startsWith(">>")) {
                        String fileName = value.substring(2).trim();
                        CsvUtil.write(fileName, actualRows);
                        return SlimMessageUtils.passTable("Result saved to file " + fileName, expectedRows);
                    } else if (value.startsWith("<<")) {
                        String fileName = value.substring(2).trim();
                        CSVDataTable expectedRowsFromCsv = new CSVDataTable(fileName, columnTransformations);
                        List<List<String>> result = SlimMessageUtils.passTable("Expected result loaded from file " + fileName, expectedRows);
                        result.addAll(compareTable(expectedRowsFromCsv, actualRows, true).toFitnessResult());
                        return result;
                    }
                }
            }
            return compareTable(new ListDataTable(expectedRows, columnTransformations), actualRows, false).toFitnessResult();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compares two Tables
     *
     * @param expectedTable Data from CSV (prepared from DB query)
     *                      First row should be headers
     *                      Data starts from second row. So IterableTable.next() should give data
     * @param actualTable   Talend job data
     * @return table comparison result
     */
    public static TableComparisonResult compareTable(IterableTable expectedTable, IterableTable actualTable, final boolean truncate) {
        LOGGER.info("Comparing tables with truncate= {}", truncate);
        TableComparisonResult result = new TableComparisonResult(truncate);
        Iterator<DataRow> expectedDataRows = expectedTable.iterator();
        Iterator<DataRow> actualDataRows = actualTable.iterator();
        result.add(compareHeaders(expectedTable.getHeaders(), actualTable.getHeaders()));
        int rowNum = 1;
        while (expectedDataRows.hasNext() || actualDataRows.hasNext()) {
            DataRow expectedRow = expectedDataRows.next();
            DataRow actualRow = actualDataRows.next();
            if (expectedRow instanceof WrongRow || actualRow instanceof WrongRow) {
                LOGGER.info("Adding Wrong row for rowNum: {}", rowNum);
                result.add(expectedRow, actualRow, rowNum);
            } else {
                result.add(compareRows(expectedRow, actualRow, rowNum));
            }
            rowNum++;
        }
        result.addComparisonSummaryMessage(rowNum);
        return result;
    }

    static RowComparisonResult compareHeaders(List<String> expectedHeaders, List<String> actualHeaders) {
        ValueComparator<String, String> headerValueComparator = new HeaderValueComparator();
        RowComparisonResult result = new RowComparisonResult();
        for (int index = 0; index < Math.max(getSize(expectedHeaders), getSize(actualHeaders)); index++) {
            result.add(headerValueComparator.compare(getHeader(expectedHeaders, index), getHeader(actualHeaders, index)));
        }
        result.add(getRowReportMessage(SlimMessageUtils.report("" + 0)));
        return result;
    }

    /**
     * @param expectedRow Contains data for all cells in that row
     * @param actualRow   Actual data
     * @param rowNum      expected data
     * @return comparison result
     */
    static RowComparisonResult compareRows(DataRow expectedRow, DataRow actualRow, int rowNum) {
        RowComparisonResult result = new RowComparisonResult();
        for (int index = 0; index < Math.max(getSize(expectedRow.getCells()), getSize(actualRow.getCells())); index++) {
            result.add(compareCellValues(expectedRow.getCell(index), actualRow.getCell(index)));
        }
        result.add(getRowReportMessage(SlimMessageUtils.report("" + rowNum)));
        return result;
    }

    static <V1, V2> CellComparisonResult compareCellValues(DataRow.Cell<V1> expectedValue, DataRow.Cell<V2> actualValue) {
        ValueComparator comparator = ComparatorFactory.getComparator(expectedValue.getColumn().getTransformation());
        return comparator.compare(getValue(expectedValue), getValue(actualValue));
    }

    private static CellComparisonResult getRowReportMessage(String report) {
        return new CellComparisonResult(false, report);
    }

    private static <T> int getSize(List<T> list) {
        if (list == null) {
            return 0;
        } else {
            return list.size();
        }
    }

    private static String getHeader(List<String> list, int index) {
        if (index < getSize(list)) {
            return list.get(index);
        } else {
            return null;
        }
    }

    private static <T> T getValue(DataRow.Cell<T> cell) {
        if (cell == null) {
            return null;
        } else {
            return cell.getValue();
        }
    }
}
