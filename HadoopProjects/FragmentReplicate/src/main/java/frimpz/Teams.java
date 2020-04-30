package frimpz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Map reduce class for teams file.
 */
public class Teams {

    public static class TeamMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text teamID = new Text("");
        Text teamnfo = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] values = value.toString().split(",");
                teamID.set(values[3]);
                teamnfo.set(createInfo(values[0],values[2],values[19]));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }

            context.write(teamID, teamnfo);
        }


        public Text createInfo(String year, String franchID, String HR){
            return new Text("team"+","+year +","+ franchID+","+ HR);
        }


    }

    public static class FranchMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text playerID = new Text("");
        Text playerInfo = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] values = value.toString().split(",");
                playerID.set(values[0]);
                playerInfo.set(createInfo(values[1]));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }

            context.write(playerID, playerInfo);
        }


        public Text createInfo(String franchName){
            return new Text("franch"+","+franchName);
        }


    }

    public static class TeamReducer extends Reducer<Text,Text, NullWritable, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> team = new ArrayList<>();
            List<String> franch = new ArrayList<>();
            NullWritable out = NullWritable.get();

            for (Text val : values){
                String[] splits =  val.toString().split(",",2);
                if(splits[0].equalsIgnoreCase("team")){
                    team.add(splits[1]);
                }else{
                    franch.add(splits[1]);
                }
            }

            for(String x: team){
                for(String y: franch){
                    context.write(out, new Text(key+","+x+","+ y));
                }
            }


        }
    }

    public static Job getTeamsJob(Configuration conf, String inputPath1, String inputPath2, String outputPath)
            throws IOException {
        Job job = Job.getInstance(conf, "Job Two");
        job.setJarByClass(Main.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, Teams.TeamMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, Teams.FranchMapper.class);
        job.setReducerClass(Teams.TeamReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
}
