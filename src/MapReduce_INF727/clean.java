package MapReduce_INF727;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class clean {

    public static void main(String[] args) throws InterruptedException {
        //function use to remove my temporary file on all machines

        String machine;
        Process p = null;
        try {
            BufferedReader br;
            br = new BufferedReader(new FileReader("Machines_TP.csv"));
            while ((machine = br.readLine()) != null) {
                System.out.println(machine);
                machine = "rgenet@" + machine;
                String[] commande = {"ssh", "-o StrictHostKeyChecking=no", machine, "hostname"};
                ProcessBuilder pb = new ProcessBuilder(commande);
                pb.redirectErrorStream(true);
                p = pb.start();
                BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
                String name = br2.readLine();
                if (machine.equals("rgenet@" + name)) {
                    String[] delete_dir_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "rm", "-rf", "/tmp/rgenet/"};
                    ProcessBuilder pb_clean = new ProcessBuilder(delete_dir_cmd);
                    p = pb_clean.start();
                    p.waitFor(20, TimeUnit.SECONDS);

                }
                br2.close();

            }
            br.close();
            p.destroy();
            System.out.println("terminated");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public static void clean_machine(String machine, String current_user) throws IOException, InterruptedException {
        String[] delete_dir_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "rm", "-rf", "/tmp/" + current_user + "/"};
        ProcessBuilder pb_clean = new ProcessBuilder(delete_dir_cmd);
        Process p = pb_clean.start();
        p.waitFor();
        p.destroy();

    }

    public static void clean_here(String folder) throws IOException, InterruptedException {
        String[] delete_dir_cmd = {"rm", "-rf", folder};
        ProcessBuilder pb_clean = new ProcessBuilder(delete_dir_cmd);
        Process p = pb_clean.start();
        p.waitFor();
        p.destroy();
    }
}
