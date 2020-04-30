package frimpz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static frimpz.FragmentReplicate.getFragmentReplicateJob;
import static frimpz.HomeRun.getHomeRunJob;
import static frimpz.IdentityMapReduce.getIdentityMapReduceJob;
import static frimpz.Players.getPlayersJob;
import static frimpz.Teams.getTeamsJob;


public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        /**
         * Getiing the various jobs created
         * Args which specify input and output files.
         * Each number specifies the argument number.
         * 0 -- people.csv, 1 -- batting.csv, 2 -- Join of people and batting
         * 3 -- teams.csv, 4 -- teamsfranchises.csv, 5 -- Join of team and franchise
         * 6 -- homerun aggregate, 7 -- final output from all reducesr, 8 --final output combined
         * 9 args for all the above in same order,
         * 4 args for only input files
         * 0 args for defaults
         */
        String x = "/user/baseball/";
        String pple = x + "People.csv", bat = x+ "Batting.csv",  team = x+"Teams.csv", teamfr = x+ "TeamsFranchises.csv";
        String ag1 = "/users/boadu_temp1", arg2 = "/users/boadu_temp2";
        String ag3 = "/users/boadu_temp3", ag4 = "/users/boadu_temp4" ,ag5 = "/users/boadu";
        if(args.length == 9){
            pple = args[0];  bat = args[1]; team = args[2]; teamfr = args[3];
            ag1 = args[4]; arg2 = args[5];ag3 = args[6]; ag4 = args[7];
            ag5 = args[8];
        }
        else if(args.length == 4){
            pple = args[0];  bat = args[1]; team = args[2]; teamfr = args[3];
        }


        Job firstJob = getPlayersJob(conf, pple, bat, ag1);
        Job secondJob = getTeamsJob(conf, team, teamfr , arg2);
        Job thirdJob = getHomeRunJob(conf, ag1, ag3);
        Job fourthJob = getFragmentReplicateJob(conf, ag3, arg2, ag4);



        firstJob.waitForCompletion(true);
        secondJob.waitForCompletion(true);
        thirdJob.waitForCompletion(true);
        fourthJob.waitForCompletion(true);


        List<ControlledJob> order = new ArrayList<>();
        List<ControlledJob> order1 = new ArrayList<>();

        ControlledJob firstControlledJob = new ControlledJob(firstJob,null);
        order.add(firstControlledJob);

        ControlledJob secondControlledJob = new ControlledJob(secondJob,null);
        ControlledJob thirdControlledJob = new ControlledJob(thirdJob,order);
        order1.add(secondControlledJob);order1.add(thirdControlledJob);

        ControlledJob fourthControlledJob = new ControlledJob(fourthJob,order1);


        JobControl jc = new JobControl("job_chaining");
        jc.addJob(firstControlledJob);
        jc.addJob(secondControlledJob);
        jc.addJob(thirdControlledJob);
        jc.addJob(fourthControlledJob);

        /**
         * Optional to combine all files from each reducer task
         */

       Job identity = getIdentityMapReduceJob(conf, ag4, ag5);
        identity.waitForCompletion(true);
        List<ControlledJob> order2 = new ArrayList<>();
        order2.add(fourthControlledJob);
        ControlledJob IdentityMapReduceJob = new ControlledJob(fourthJob,order2);
        jc.addJob(IdentityMapReduceJob);


        //jc.run();


        deleteIntermeduateFiles(conf, ag1);
        deleteIntermeduateFiles(conf, arg2);
        deleteIntermeduateFiles(conf, ag3);
        deleteIntermeduateFiles(conf, ag4);

    }

    public static void deleteIntermeduateFiles(Configuration conf, String s){
        Path path = new Path(s);
        try {
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.delete(path, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
