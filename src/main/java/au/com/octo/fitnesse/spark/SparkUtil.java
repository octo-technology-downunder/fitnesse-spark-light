package au.com.octo.fitnesse.spark;

import au.com.octo.fitnesse.fixtures.utils.Constants;
import au.com.octo.fitnesse.fixtures.utils.UDF;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SparkUtil {

    private static final Charset CHARSET = Charset.forName("UTF-8");
    private final static Logger LOGGER = LoggerFactory.getLogger(SparkUtil.class);
    private final static CSVFormat FORMAT = CSVFormat.newFormat('|').withQuote('"').withRecordSeparator(Constants.RECORD_SEPARATOR);

    public static void writeCsv(Dataset<Row> dataset, String file) throws IOException {
        File tmpFolder = new File(file + ".tmp");
        if (tmpFolder.exists()) {
            throw new RuntimeException("Folder " + tmpFolder.toString() + " already exists");
        }
        tmpFolder.deleteOnExit();
        tmpFolder.mkdirs();
        LOGGER.debug("Writing partitions to " + tmpFolder);
        if (LOGGER.isDebugEnabled()) {
            dataset.explain();
        }
        String[] headers = dataset.columns();
        dataset.coalesce(1).foreachPartition(partition -> {
            File part = new File(tmpFolder, "part_" + partition.hashCode());
            try (CSVPrinter printer = csvPrinter(part)) {
                printer.printRecord((Object[]) headers);
                partition.forEachRemaining(row -> {
                    try {
                        for (int i = 0; i < row.length(); i++) {
                            printer.print(row.getString(i));
                        }
                        printer.println();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        });
        LOGGER.debug("Merging files to " + tmpFolder);
        int rowNum = 0;
        try (CSVPrinter printer = csvPrinter(file)) {
            for (File tmpFile : tmpFolder.listFiles()) {
                if (tmpFile.getName().startsWith("part_")) {
                    int partRowNum = 0;
                    try (CSVParser csvParser = csvParser(tmpFile)) {
                        for (CSVRecord csvRecord : csvParser) {
                            if (rowNum <= 0 || partRowNum != 0) {
                                printer.printRecord(csvRecord);
                            }
                            partRowNum++;
                            rowNum++;
                        }
                    }
                }
            }
        }
        FileUtils.deleteDirectory(tmpFolder);
        LOGGER.debug("Finished, " + rowNum + " rows written");
    }

    public static List<StructField> getSchemaFromHeader(Map<String, Integer> headerMap) {
        List<StructField> fields = new ArrayList<>();
        for (String s : headerMap.keySet()) {
            DataType dataType = DataTypes.StringType;

            String[] nameTypePair = s.split(":");
            String name = nameTypePair[0];

            if (nameTypePair.length == 2) {
                String type = nameTypePair[1];

                switch (type) {
                    case "Int":
                        dataType = DataTypes.IntegerType;
                        break;
                    case "Date":
                        dataType = DataTypes.DateType;
                        break;
                    case "Double":
                        dataType = DataTypes.DoubleType;
                        break;
                    case "Decimal":
                        dataType = DataTypes.createDecimalType(10, 2);
                        break;
                    case "String":
                        break;
                    default:
                        // accept struct arrays, preserve the type in the name, will convert later
                        if (StructsUtil.isArrayType(type)) {
                            name = name + ":" + type;
                            break;
                        } else throw new IllegalArgumentException("Type is not supported " + type);
                }
            }

            StructField field = DataTypes.createStructField(name, dataType, true);
            fields.add(field);
        }

        return fields;
    }

    public static Dataset readCsv(SparkSession sparkSession, StructType schema, String... files) {
        return sparkSession
                .read()
                .option("header", "true")
                .option("multiLine", "true")
                .option("delimiter", "|")
                .schema(schema)
                .csv(files);
    }

    public static SparkSession createSparkSession(Map<String, String> conf) {
        System.setProperty("line.separator", Constants.RECORD_SEPARATOR);
        SparkConf sparkConf = new SparkConf()
                .set("spark.debug.maxToStringFields", "100000")
                .set("spark.ui.showConsoleProgress", "false");
        conf.forEach(sparkConf::set);
        SparkSession session = SparkSession.builder()
                .appName("Fitnesse run")
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate();
        registerUDFs(session);
        return session;
    }

    private static void registerUDFs(SparkSession sparkSession) {
        sparkSession
                .udf()
                .register("timestampToString", UDF.timestampToString, DataTypes.StringType);
    }

    private static CSVPrinter csvPrinter(File file) throws IOException {
        OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file), CHARSET);
        return new CSVPrinter(out, FORMAT);
    }

    private static CSVPrinter csvPrinter(String file) throws IOException {
        return csvPrinter(new File(file));
    }

    private static CSVParser csvParser(File file) throws IOException {
        return CSVParser.parse(file, CHARSET, FORMAT);
    }

}
