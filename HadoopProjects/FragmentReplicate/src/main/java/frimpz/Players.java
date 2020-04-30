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
 * Mapreduce class for players file.
 */
public class Players {
    public static class PeopleMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text playerID = new Text("");
        Text playerInfo = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] values = value.toString().split(",");
                playerID.set(values[0]);
                playerInfo.set(createInfo(values[13],values[14]));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }

            context.write(playerID, playerInfo);
        }


        public Text createInfo(String firstName, String lastName){
            return new Text("People"+","+firstName +","+ lastName);
        }


    }

    public static class BattingMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text playerID = new Text("");
        Text playerInfo = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] values = value.toString().split(",");
                playerID.set(values[0]);
                playerInfo.set(createInfo(values[1],values[11]));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }

            context.write(playerID, playerInfo);
        }


        public Text createInfo(String yearID, String HR){
            return new Text("Batting"+","+yearID +","+ HR);
        }


    }

    public static class PlayerReducer extends Reducer<Text,Text, NullWritable, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> batting = new ArrayList<>();
            List<String> people = new ArrayList<>();
            NullWritable out = NullWritable.get();

            for (Text val : values){
                String[] splits =  val.toString().split(",",2);
                if(splits[0].equalsIgnoreCase("Batting")){
                    batting.add(splits[1]);
                }else{
                    people.add(splits[1]);
                }
            }

            for(String x: batting){
                for(String y: people){
                    context.write(out, new Text(key+","+x+","+ y));
                }
            }


        }
    }

    public static Job getPlayersJob(Configuration conf, String inputPath1, String inputPath2, String outputPath)
            throws IOException {
        Job job = Job.getInstance(conf, "Job One");
        job.setJarByClass(Main.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, PeopleMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, BattingMapper.class);
        job.setReducerClass(PlayerReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
}
