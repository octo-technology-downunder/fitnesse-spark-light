package au.com.octo.fitnesse.fixtures.utils;

import au.com.octo.fitnesse.domain.IterableTable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CsvUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsvUtil.class);
    public final static CSVFormat FORMAT = CSVFormat.newFormat('|').withQuote('"').withRecordSeparator(Constants.RECORD_SEPARATOR);

    public static Iterator<List<String>> read(String fileName) {
        File file = new File(Constants.OUTPUT + fileName);
        try {
            CSVParser parser = CSVParser.parse(file, Constants.CHARSET, FORMAT);
            return new Iterator<List<String>>() {
                private Iterator<CSVRecord> iterator = parser.iterator();
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
                public List<String> next() {
                    if (!hasNext())
                        throw new RuntimeException("End of file reached");
                    CSVRecord record = iterator.next();
                    List<String> next = new ArrayList<>();
                    record.forEach(value -> next.add(value));
                    return next;
                }
            };
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    public static void write(String file, Iterator<List<String>> records) throws IOException {
        try (CSVPrinter printer = csvPrinter(file)) {
            records.forEachRemaining(record -> {
                try {
                    printer.printRecord(record);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            FileUtil.unlockOutputFile(file);
        }
    }

    public static void write(String file, IterableTable records) throws IOException {
        LOGGER.info("Writing records to file: {}", file);
        try (CSVPrinter printer = csvPrinter(file)) {
            printer.printRecord(records.getHeaders());
            records.iterator().forEachRemaining(record -> {
                try {
                    printer.printRecord(record.getCellValues());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } finally {
            FileUtil.unlockOutputFile(file);
        }
    }

    public static CSVPrinter csvPrinter(String file) {
        try {
            File outputFile = FileUtil.prepareOutputFile(file);
            OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8");
            return new CSVPrinter(out, FORMAT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static long rowCount(String file) {
        try {
            CSVParser parser = CSVParser.parse(new File(file), Constants.CHARSET, FORMAT);
            long i = 0;
            for (Iterator<CSVRecord> iterator = parser.iterator(); iterator.hasNext(); ) {
                iterator.next();
                i++;
            }
            return i;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
