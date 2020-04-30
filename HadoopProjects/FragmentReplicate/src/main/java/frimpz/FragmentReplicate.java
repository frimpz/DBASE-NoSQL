package frimpz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;


/**
 * This class performs a fragment and replicate join over the output of plaayer homerun and teams homerun
 * It reads each file line and by and assigns key one and appends player to each line in player file
 * It reads each file line and by and assigns key two and appends team to each line in team file
 */

public class FragmentReplicate {

    public static class playerMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text one = new Text("one");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                context.write(one, new Text("player,"+value));
                context.write(one, new Text("player,"+value));
                context.write(one, new Text("player,"+value));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }
        }


    }

    public static class teamMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text two = new Text("two");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                context.write(two, new Text("team,"+value));
                context.write(two, new Text("team,"+value));
                context.write(two, new Text("team,"+value));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }
        }


    }


    public static class BasicReducer extends Reducer<Text,Text,NullWritable, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            NullWritable out = NullWritable.get();
            List<String> teams = new ArrayList<>();
            List<String> players = new ArrayList<>();

            for (Text val : values){
                String[] splits =  val.toString().split(",",2);
                if(splits[0].equalsIgnoreCase("team")){
                    teams.add(splits[1]);
                }else{
                    players.add(splits[1]);
                }
            }


            for(String x: players){
                String[] player = x.split(",");
                for(String y: teams){
                    String[] team = y.split(",");
                    if(myParseInt(player[2])>myParseInt(team[3]) && team[1].equals(player[1])){
                        context.write(out, new Text(team[4]+" "+player[3]+" "+player[4]+" "+team[1]));
                    }
                }
            }

        }

        public int myParseInt(String val){
            try{
                return Integer.parseInt(val);
            }catch (NumberFormatException e){
                return 0;
            }
        }
    }


    /**
     * Custom partitioner to handle which reduce task handles each map output.
     * THe partitioner receives the out of each map and sends them to each reducer
     * 0,1,2,3,4,5,6,7,8 -- order for distributing map outputs from file 1
     * 0,3,6,1,4,7,2,5,8 -- order for distributing map output from file 2.
     */
    public static class BasicPartitioner extends Partitioner<Text, Text>{

        int file1 = 0;
        int file2 = 0;
        int count = 0;
        int redNo;

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {

            if(numReduceTasks == 0)
            {
                return 0;
            }
            if(key.equals(new Text("one")))
            {
                redNo = file1%numReduceTasks;
                file1++;
                return redNo;
            }
            else
            {
                redNo = file2%numReduceTasks;
                file2+=3;
                if(file2 >=9 ){
                    count = (count+1)%3 ;
                    file2 = count;
                }
                return redNo;
            }

        }
    }

    /**
     *  Although multiple lines from same output will be sent to the same reduce task, they will still be grouped by the keys(one,two) given to each line.
     *  This custom comparator is used to overidde the groupings.
     */
    public static  class MyKeyComparator implements RawComparator<Text> {

        @Override
        public int compare(Text players, Text team) {
            return 0;
        }

        @Override
        public int compare(byte[] bytes, int i, int i1, byte[] bytes1, int i2, int i3) {
            return 0;
        }
    }

    /**
     * Method for fragment and replicate.
     * @param conf
     * @param inputPath1
     * @param inputPath2
     * @param outputPath
     * @return
     * @throws IOException
     */
    public static Job getFragmentReplicateJob(Configuration conf, String inputPath1, String inputPath2, String outputPath)
            throws IOException {

        Job job = Job.getInstance(conf, "Fragment and Replicate");
        job.setJarByClass(FragmentReplicate.class);
        job.setPartitionerClass(BasicPartitioner.class);

        Path path = new Path(inputPath1);
        Path path2 = new Path(inputPath2);
        Path finalOutput = new Path(outputPath);

        MultipleInputs.addInputPath(job, path, TextInputFormat.class, playerMapper.class);
        MultipleInputs.addInputPath(job, path2, TextInputFormat.class, teamMapper.class);


        job.setReducerClass(BasicReducer.class);
        // Setting number of reduce task
        job.setNumReduceTasks(9);
        //Setting grouping comparator
        job.setGroupingComparatorClass(MyKeyComparator.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        FileOutputFormat.setOutputPath(job, finalOutput);
        return job;
    }

}
