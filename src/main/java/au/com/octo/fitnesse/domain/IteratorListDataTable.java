package au.com.octo.fitnesse.domain;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class IteratorListDataTable implements IterableTable {

    private Iterator<List<String>> listIterator;

    private List<String> headers;

    public IteratorListDataTable(Iterator<List<String>> listIterator) {
        this.listIterator = listIterator;
        this.headers = listIterator.next();
    }

    @Override
    public List<String> getHeaders() {
        return headers;
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public DataRow next() {
                if (listIterator.hasNext()) {
                    List<String> values = listIterator.next();
                    DataRow dataRow = new DataRow();
                    AtomicInteger index = new AtomicInteger(0);
                    List<DataRow.Cell> entities = values.stream().map(value -> {
                        String columnName = headers.get(index.getAndIncrement());
                        Column column = new Column(columnName, Transformation.UNKNOWN);
                        DataRow.Cell entity = dataRow.new Cell(value, column);
                        return entity;
                    }).collect(Collectors.toList());
                    dataRow.setCells(entities);
                    return dataRow;
                } else {
                    return new MissingRow();
                }
            }
        };
    }
}