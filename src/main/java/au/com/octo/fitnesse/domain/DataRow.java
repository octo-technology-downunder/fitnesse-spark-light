package au.com.octo.fitnesse.domain;

import java.util.ArrayList;
import java.util.List;

public class DataRow {

    private List<Cell> cells;

    public DataRow() {
        this.cells = new ArrayList<>();
    }

    public List<Cell> getCells() {
        return cells;
    }

    public List<Object> getCellValues() {
        List<Object> obj = new ArrayList<>();
        for (Cell cell : cells) {
            obj.add(cell.getValue());
        }
        return obj;
    }

    public void setCells(List<Cell> cells) {
        this.cells = cells;
    }

    public Cell getCell(int index) {
        if (index < cells.size()) {
            return cells.get(index);
        } else {
            return null;
        }
    }

    public Object getCellValue(int index) {
        return getCell(index).getValue();
    }

    /**
     * This class contains cell Value and transformation required for it.
     */
    public class Cell<T> {

        private T value;
        private Column column;

        public Cell(T value, Column column) {
            this.value = value;
            this.column = column;
        }

        String getValueAsString() {
            if (value == null) {
                return null;
            } else {
                return value.toString();
            }
        }
        public T getValue() {
            return value;
        }

        public Column getColumn() {
            return column;
        }
    }
}
