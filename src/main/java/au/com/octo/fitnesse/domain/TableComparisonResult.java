package au.com.octo.fitnesse.domain;

import au.com.octo.fitnesse.fixtures.utils.SlimMessageUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static au.com.octo.fitnesse.fixtures.utils.Constants.*;

public class TableComparisonResult {

    private boolean truncate;
    private final List<RowComparisonResult> rowComparisonResults;
    private final List<RowComparisonResult> rowComparisonErrors;
    private int nbErrors = 0;

    public TableComparisonResult(boolean truncate) {
        this.truncate = truncate;
        this.rowComparisonResults = new LinkedList<>();
        this.rowComparisonErrors = new LinkedList<>();
    }

    public List<List<String>> toFitnessResult() {
        return rowComparisonResults.stream()
                .map(row -> row.getCellComparisonResultList().stream().map(CellComparisonResult::getMessage).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    public void add(RowComparisonResult rowComparisonResult) {
        boolean isError = rowComparisonResult.getCellComparisonResultList().stream().anyMatch(CellComparisonResult::isError);
        if (!truncate || rowComparisonResults.size() < DISPLAY_ROWS) {
            rowComparisonResults.add(rowComparisonResult);
        } else if (isError && nbErrors < DISPLAY_ERRORS) {
            rowComparisonErrors.add(rowComparisonResult);
        }
        if (isError) {
            this.nbErrors++;
        }
    }

    public void add(DataRow expectedRow, DataRow actualRow, int rowNum) {
        String message;
        RowComparisonResult rowComparisonResult = new RowComparisonResult();
        List<CellComparisonResult> cellComparisonResults = null;

        if (actualRow instanceof MissingRow) {
            List<DataRow.Cell> cells = expectedRow.getCells();
            message = MISSING_ROW;
            cellComparisonResults = prepareCellResultWithMessage(rowNum, cells, message);
        } else if (expectedRow instanceof MissingRow) {
            List<DataRow.Cell> cells = actualRow.getCells();
            message = UNEXPECTED_ROW;
            cellComparisonResults = prepareCellResultWithMessage(rowNum, cells, message);
        } else if (actualRow instanceof WrongRow) {
            List<DataRow.Cell> cells = expectedRow.getCells();
            message = MULTIPLE_MATCHING_ROWS;
            cellComparisonResults = prepareCellResultWithMessage(rowNum, cells, message);
        }
        rowComparisonResult.addAll(cellComparisonResults);
        add(rowComparisonResult);
    }

    public void addComparisonSummaryMessage(int rowNum) {
        if (truncate && rowNum > DISPLAY_ROWS) {
            rowComparisonResults.add(getReportTruncatedMessage());
        }
        if (CollectionUtils.isNotEmpty(this.rowComparisonErrors)) {
            rowComparisonResults.addAll(this.rowComparisonErrors);
        }
        if (nbErrors > DISPLAY_ERRORS) {
            rowComparisonResults.add(getReportTruncatedMessage());
        }

        RowComparisonResult rowComparisonResult = new RowComparisonResult();
        List<CellComparisonResult> reportRow = new ArrayList<>();
        CellComparisonResult cellComparisonResult;
        if (nbErrors > 0) {
            cellComparisonResult = new CellComparisonResult(false, SlimMessageUtils.fail("Total " + rowNum + " rows " + nbErrors + " errors"));
        } else {
            cellComparisonResult = new CellComparisonResult(false, SlimMessageUtils.pass("Total " + rowNum + " rows no error"));
        }
        reportRow.add(cellComparisonResult);
        rowComparisonResult.addAll(reportRow);
        rowComparisonResults.add(rowComparisonResult);
    }

    private static RowComparisonResult getReportTruncatedMessage() {
        RowComparisonResult result1 = new RowComparisonResult();
        result1.add(new CellComparisonResult(false, (SlimMessageUtils.report("..."))));
        return result1;
    }

    private <T> List<CellComparisonResult> prepareCellResultWithMessage(int rowNum, List<DataRow.Cell> cells, String message) {
        List<CellComparisonResult> cellComparisonResults = new ArrayList<>();
        cellComparisonResults.addAll(cells.stream().map(t -> new CellComparisonResult(false, SlimMessageUtils.report(t.getValueAsString()))).collect(Collectors.toList()));
        cellComparisonResults.add(new CellComparisonResult(false, SlimMessageUtils.report("" + rowNum)));
        cellComparisonResults.add(new CellComparisonResult(true, SlimMessageUtils.fail(message)));
        return cellComparisonResults;
    }
}
