package au.com.octo.fitnesse.domain;

import java.util.LinkedList;
import java.util.List;

public class RowComparisonResult {

    private List<CellComparisonResult> cellComparisonResultList;

    public RowComparisonResult() {
        this.cellComparisonResultList = new LinkedList<>();
    }

    public void add(CellComparisonResult cellComparisonResult) {
        this.cellComparisonResultList.add(cellComparisonResult);
    }

    public void addAll(List<CellComparisonResult> cellComparisonResultList) {
        this.cellComparisonResultList.addAll(cellComparisonResultList);
    }

    public List<CellComparisonResult> getCellComparisonResultList() {
        return cellComparisonResultList;
    }
}
