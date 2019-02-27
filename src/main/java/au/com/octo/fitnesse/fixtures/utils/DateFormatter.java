package au.com.octo.fitnesse.fixtures.utils;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateFormatter {

    public static final DateTimeFormatter formatStandard = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

}
