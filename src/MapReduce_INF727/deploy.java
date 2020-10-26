package MapReduce_INF727;

import java.io.IOException;

public class deploy {

    public static void deploy_file(String machine, String file, String destination) throws IOException, InterruptedException {

        if (machine != null) {
            machine += ":";
        } else {
            machine = "";
        }

        String[] copy_cmd = {"scp", "-o StrictHostKeyChecking=no", "-r", "-p", file, machine + destination};
        ProcessBuilder pb_copy = new ProcessBuilder(copy_cmd);
        Process p = pb_copy.start();
        p.waitFor();
        p.destroy();
    }

}
