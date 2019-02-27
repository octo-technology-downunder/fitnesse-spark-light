package au.com.octo.fitnesse.domain;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ListDataTable implements IterableTable {

    private List<List<String>> list;
    private int index;
    private Map<String, String> transformation;

    public ListDataTable(List<List<String>> list) {
        this.list = list;
        setHeaders();
        index = 1;
    }

    public ListDataTable(List<List<String>> list, Map<String, String> transformation) {
        this.list = list;
        setHeaders();
        index = 1;
        this.transformation = transformation;
    }

    private List<String> headers = new LinkedList();

    @Override
    public List<String> getHeaders() {
        return headers;
    }

    private List<String> setHeaders() {
        return this.headers = list.get(index);
    }

    @Override
    public Iterator<DataRow> iterator() {

        return new Iterator<DataRow>() {
            @Override
            public boolean hasNext() {
                if (index < list.size()) {
                    return true;
                }
                return false;
            }

            @Override
            public DataRow next() {
                if (hasNext()) {
                    List<String> data = list.get(index);
                    DataRow dataRow = new DataRow();
                    AtomicInteger counter = new AtomicInteger(0);
                    List<DataRow.Cell> entities = data.stream().map(rowData -> {
                        String columnName = headers.get(counter.getAndIncrement());
                        DataRow.Cell entity = dataRow.new Cell(rowData, new Column(columnName, getTransformation(columnName, transformation)));
                        return entity;
                    }).collect(Collectors.toList());
                    dataRow.setCells(entities);
                    index = index + 1;
                    return dataRow;
                } else {
                    return new MissingRow();
                }
            }
        };
    }
}
