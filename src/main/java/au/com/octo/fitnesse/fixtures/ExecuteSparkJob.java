package au.com.octo.fitnesse.fixtures;

import au.com.octo.fitnesse.fixtures.utils.*;
import au.com.octo.fitnesse.spark.SparkJob;
import au.com.octo.fitnesse.spark.SparkUtil;
import au.com.octo.fitnesse.spark.StructsUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

@SuppressWarnings("rawtypes")
public class ExecuteSparkJob {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExecuteSparkJob.class);

    private final SparkJob sparkJob;
    private final List datasetNames;
    private final String outputFile;
    private Timestamp sysTimestamp;
    private String[] orderByColumns;
    private final Map<String, String> config;

    public ExecuteSparkJob(ExecuteSparkJobBuilder builder) throws Exception {
        this.sparkJob = builder.sparkJob;
        this.datasetNames = builder.datasetNames;
        this.outputFile = builder.outputFile;
        this.sysTimestamp = builder.sysTimestamp;
        this.orderByColumns = builder.orderByColumns;
        this.config = builder.config;
    }

    public ExecuteSparkJob(String className) throws Exception {
        this(new ExecuteSparkJobBuilder(className));
    }

    public ExecuteSparkJob(String className, String outputFile) throws Exception {
        this(new ExecuteSparkJobBuilder(className).outputFile(outputFile));
    }

    public ExecuteSparkJob(String className, String outputFile, String sysTimestamp) throws Exception {
        this(new ExecuteSparkJobBuilder(className).outputFile(outputFile).sysTimestamp(sysTimestamp));
    }

    public ExecuteSparkJob(String className, String outputFile, String orderByColumns, String sysTimestamp) throws Exception {
        this(new ExecuteSparkJobBuilder(className).outputFile(outputFile).orderByColumns(orderByColumns).sysTimestamp(sysTimestamp));
    }

    public ExecuteSparkJob(String className, String outputFile, String orderByColumns, String sysTimestamp, String config) throws Exception {
        this(new ExecuteSparkJobBuilder(className).outputFile(outputFile).orderByColumns(orderByColumns).sysTimestamp(sysTimestamp).config(config));
    }

    public List<List<String>> doTable(List<List<String>> files) throws Exception {
        boolean datasetsOk = true;
        List<List<String>> result = new ArrayList<>();
        Iterator<List<String>> datasetFilesIterator = files.iterator();
        Iterator<String> datasetNamesIterator = datasetNames.iterator();
        SparkSession sparkSession = SparkUtil.createSparkSession(config);
        try {
            while (datasetFilesIterator.hasNext() || datasetNamesIterator.hasNext()) {
                List<String> resultRow = new ArrayList<>();

                if (datasetNamesIterator.hasNext()) {
                    String datasetName = datasetNamesIterator.next();

                    if (datasetFilesIterator.hasNext()) {
                        String datasetFile = "";
                        resultRow.add(SlimMessageUtils.pass("Dataset " + datasetName));

                        List<String> filesList = datasetFilesIterator.next();

                        assert (filesList.size() == 1);

                        String fileName = filesList.get(0);

                        File file = new File(Constants.OUTPUT + fileName);

                        if (!file.exists()) {
                            datasetsOk = false;
                            resultRow.add(SlimMessageUtils.fail("File not found: " + fileName));
                        } else {
                            datasetFile = Constants.OUTPUT + fileName;
                            resultRow.add(SlimMessageUtils.pass(fileName));
                        }

                        if (datasetsOk) {
                            Reader csv = new FileReader(datasetFile);
                            Map<String, Integer> csvHeader = CsvUtil.FORMAT.withHeader().parse(csv).getHeaderMap();
                            List<StructField> fields = SparkUtil.getSchemaFromHeader(csvHeader);
                            Dataset ds = SparkUtil.readCsv(sparkSession, DataTypes.createStructType(fields), datasetFile);
                            StructsUtil.unstringifyStructs(ds).createTempView(datasetName);
                        }

                    } else {
                        datasetsOk = false;
                        resultRow.add(SlimMessageUtils.fail("Dataset " + datasetName + " is missing"));
                    }

                } else {
                    datasetsOk = false;
                    resultRow.add(SlimMessageUtils.fail("Not enough arguments"));
                    List<String> filesList = datasetFilesIterator.next();
                    filesList.forEach(file -> resultRow.add(SlimMessageUtils.fail(file)));
                }
                result.add(resultRow);
            }

            List<String> resultRow = new ArrayList<>();

            if (datasetsOk) {
                Dataset executeResultDS = sparkJob.execute(sparkSession, this.sysTimestamp);
                Dataset<Row> output = transformDataset(StructsUtil.stringifyStrucs(executeResultDS));
                String outputCsv = outputFile != null ? outputFile : sparkJob.getClass().getSimpleName();
                try {
                    SparkUtil.writeCsv(output, FileUtil.prepareOutputFile(outputCsv).toString());
                } finally {
                    FileUtil.unlockOutputFile(outputCsv);
                }
                resultRow.add(SlimMessageUtils.pass("Result saved to " + Constants.OUTPUT + outputCsv));
            } else {
                resultRow.add(SlimMessageUtils.fail(":'( could not run Spark job"));
            }
            result.add(resultRow);
        } catch (Exception e) {
            LOGGER.error("Spark job " + sparkJob.getClass().getName() + " failed", e);
            throw e;
        } finally {
            sparkSession.stop();
        }
        LOGGER.info("Spark job " + sparkJob.getClass().getName() + " succeeded");

        return result;
    }

    private Dataset<Row> transformDataset(Dataset resultDS) {
        Dataset orderedDS;
        if (this.orderByColumns.length > 0) {
            orderedDS = resultDS.orderBy(Arrays.stream(this.orderByColumns).map(string -> col(string)).toArray(i -> new Column[i]));
        } else {
            orderedDS = resultDS;
        }

        // Cast every column to String
        List<StructField> structFields = JavaConversions.seqAsJavaList(orderedDS.schema().toList());

        List<Column> columns = structFields.stream().map(new Function<StructField, Column>() {

            @Override
            public Column apply(StructField structField) {
                String fieldName = structField.name();

                if (structField.dataType() == TimestampType) {
                    return callUDF("timestampToString", col(fieldName)).as(fieldName);
                } else {
                    return col(fieldName).cast(StringType).as(fieldName);
                }
            }

            @Override
            public <V> Function<V, Column> compose(Function<? super V, ? extends StructField> before) {
                return null;
            }
        }).collect(Collectors.toList());

        Seq<Column> columnsSeq = JavaConversions.asScalaBuffer(columns);
        return orderedDS.select(columnsSeq);
    }

    public static class ExecuteSparkJobBuilder {

        private SparkJob sparkJob;
        private final List datasetNames;
        private String outputFile;
        private Timestamp sysTimestamp;
        private String[] orderByColumns = new String[]{};
        private Map<String, String> config = new LinkedHashMap<>();

        public ExecuteSparkJobBuilder(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
            Class<?> cls = Class.forName(className);
            this.sparkJob = (SparkJob) cls.newInstance();
            this.datasetNames = sparkJob.describeInput();
        }

        public ExecuteSparkJobBuilder outputFile(String outputFile) {
            this.outputFile = outputFile.replaceAll(">", "");
            return this;
        }

        public ExecuteSparkJobBuilder sysTimestamp(String sysTimestamp) {
            if (isNotBlank(sysTimestamp))
                this.sysTimestamp = new Timestamp(DateFormatter.formatStandard.parseMillis(sysTimestamp));
            else
                this.sysTimestamp = new Timestamp(System.currentTimeMillis());
            return this;
        }

        public ExecuteSparkJobBuilder config(String config) {
            for (String keyValue : config.split("[,;]")) {
                String[] pairs = keyValue.split("=", 2);
                this.config.put(pairs[0].trim(), pairs.length == 1 ? "" : pairs[1].trim());
            }
            return this;
        }

        public ExecuteSparkJobBuilder orderByColumns(String orderByColumns) {
            this.orderByColumns = orderByColumns.split("[\\s,;]+");
            return this;
        }

        public ExecuteSparkJob build() throws Exception {
            return new ExecuteSparkJob(this);
        }
    }

}

