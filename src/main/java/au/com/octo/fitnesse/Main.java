package au.com.octo.fitnesse;

import fitnesseMain.FitNesseMain;

/**
 * Fitnesse launcher.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        FitNesseMain.main(new String[] { "-v", "-p", "8081", "-r", "." });
    }
}
