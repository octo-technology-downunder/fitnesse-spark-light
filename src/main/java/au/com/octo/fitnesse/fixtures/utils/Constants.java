package au.com.octo.fitnesse.fixtures.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

public class Constants {

    public static String OUTPUT = "FitNesseRoot/files/actualFiles/"; // non-final to force initialization of this class
    public final static String BLOB = "blob";
    public static final String BINARY_DATA = "Binary Data";
    public static final String TRIM = "trim";

    static {
        new File(OUTPUT).mkdirs();
        try {
            File plugins = new File("FitnesseRoot/plugins.properties");
            if (plugins.exists()) {
                FileUtils.copyFile(plugins, new File("plugins.properties"));
            }
        } catch (IOException e) {
            //Oups
        }
    }

    public final static int DISPLAY_ROWS = 20;
    public final static int DISPLAY_ERRORS = 20;
    public final static Charset CHARSET = Charset.forName("UTF-8");

    public static final String MISSING_ROW = "Missing row";
    public static final String MULTIPLE_MATCHING_ROWS = "Multiple matching rows";
    public static final String UNEXPECTED_ROW = "Unexpected row";

    /**
     * This is the standard for the data lake DO NOT CHANGE THIS VALUE
     */
    public static final String RECORD_SEPARATOR = "\r\n";
}
