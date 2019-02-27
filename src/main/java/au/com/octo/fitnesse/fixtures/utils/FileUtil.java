package au.com.octo.fitnesse.fixtures.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

import static au.com.octo.fitnesse.fixtures.utils.Constants.OUTPUT;

public class FileUtil {
    private final static boolean deleteOnExit = new File("do_not_delete_files_on_exit").exists();

    public static File prepareOutputFile(String fileName) throws IOException {
        File file = new File(OUTPUT + fileName);
        File lockFile = new File(OUTPUT + fileName + ".lock");
        if (lockFile.exists()) {
            throw new RuntimeException("File " + file.toString() + " already exists. You should check if there isn't already another program running using this file. If not, delete this file and retry.");
        }

        file.getParentFile().mkdirs();
        FileUtils.writeStringToFile(lockFile, "", "UTF-8");
        lockFile.deleteOnExit();
        if (deleteOnExit) {
            file.deleteOnExit();
        }
        return file;
    }

    public static void unlockOutputFile(String fileName) {
        new File(OUTPUT + fileName + ".lock").delete();
    }

}
