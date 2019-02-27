package au.com.octo.fitnesse.domain;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static au.com.octo.fitnesse.fixtures.utils.Constants.*;

public class CSVDataTable implements IterableTable {

    private final static CSVFormat FORMAT = CSVFormat.newFormat('|').withQuote('"').withRecordSeparator(RECORD_SEPARATOR);

    private CSVParser parser;
    private Iterator<CSVRecord> iterator;

    private Map<String, String> transformations;
    private List<String> headers = new LinkedList<>();

    public CSVDataTable(String fileName, Map<String, String> transformations) {
        try {
            this.transformations = transformations;
            File file = new File(OUTPUT + fileName);
            parser = CSVParser.parse(file, CHARSET, FORMAT);
            this.iterator = parser.iterator();
            setHeaders();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
                if (!hasNext()) {
                    return new MissingRow();
                }

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
