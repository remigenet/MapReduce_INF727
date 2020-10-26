package MapReduce_INF727;


import java.io.IOException;
import java.util.concurrent.Callable;


public class reduce_launcher implements Callable<String> {
    //callable class used for launching the reduce phase on the cluster

    private final String machine;
    private final String number;
    private final String master_node;
    private final String current_user;

    public reduce_launcher(String m, String n, String mn, String cu) {
        this.machine = m;
        this.number = n;
        this.master_node = mn;
        this.current_user = cu;
    }

    @Override
    public String call() {
        Process p;
        String[] launch_slave_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "java", "-jar", "/tmp/" + current_user + "/slave.jar", "2", number, current_user, master_node};
        ProcessBuilder pb_launch_slave = new ProcessBuilder(launch_slave_cmd);
        pb_launch_slave.redirectErrorStream(true);
        try {
            p = pb_launch_slave.start();
            p.waitFor();
            p.destroy();
            return number + " 111";
        } catch (IOException | InterruptedException e) {
            return number + " 000";
        }


    }


}