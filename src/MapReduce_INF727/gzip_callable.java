package MapReduce_INF727;

import java.io.IOException;
import java.util.concurrent.Callable;

public class gzip_callable implements Callable<String> {
    //class used to compress file in multiprocessing
    private final String file;

    public gzip_callable(String m) {
        this.file = m;
    }

    @Override
    public String call() throws InterruptedException, IOException {

        String[] zip_cmd = {"gzip", file};
        ProcessBuilder pb_zip = new ProcessBuilder(zip_cmd);
        pb_zip.redirectError();
        Process p = pb_zip.start();
        p.waitFor();
        p.destroy();
        return null;
    }

}
