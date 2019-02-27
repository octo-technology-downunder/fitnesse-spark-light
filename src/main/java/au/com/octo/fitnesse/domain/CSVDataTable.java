package au.com.octo.fitnesse.domain;

import au.com.octo.fitnesse.fixtures.utils.Constants;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CSVDataTable implements IterableTable {

    private File file;

    private final static CSVFormat FORMAT = CSVFormat.newFormat('|').withQuote('"').withRecordSeparator(Constants.RECORD_SEPARATOR);

    private CSVParser parser = null;

    private Iterator<CSVRecord> iterator;

    private Map<String, String> transformations;

    public CSVDataTable(String fileName, Map<String, String> transformations) {
        try {
            this.transformations = transformations;
            file = new File(Constants.OUTPUT + fileName);
            parser = CSVParser.parse(file, Constants.CHARSET, FORMAT);
            this.iterator = parser.iterator();
            setHeaders();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> headers = new LinkedList<>();

    public List<String> getHeaders() {
        return headers;
    }

    @Override
    public Iterator<DataRow> iterator() {
        return new Iterator<DataRow>() {
            private boolean open = true;

            @Override
            public boolean hasNext() {
                boolean hasNext = iterator.hasNext();
                if (!hasNext && open) {
                    try {
                        parser.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    open = false;
                }
                return iterator.hasNext();
            }

            @Override
            public DataRow next() {
                if (!hasNext())
                    return new MissingRow();
                CSVRecord record = iterator.next();
                DataRow dataRow = new DataRow();

                List<DataRow.Cell> entities = new ArrayList<>();
                AtomicInteger i = new AtomicInteger(0);

                record.forEach(value -> {
                    String columnName = headers.get(i.getAndIncrement());
                    Column column = new Column(columnName, getTransformation(columnName, transformations));
                    DataRow.Cell entity = dataRow.new Cell(value, column);
                    entities.add(entity);
                });
                dataRow.setCells(entities);
                return dataRow;
            }
        };

    }

    private void setHeaders() {
        if (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            record.forEach(value -> {
                headers.add(value);
            });
        }
    }
}
