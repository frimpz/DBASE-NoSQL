package frimpz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Map reduce class for calculating home run for each player.
 */
public class HomeRun {

    public static class HRMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text ID = new Text("");
        Text Info = new Text("");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try{
                String[] values = value.toString().split(",");
                ID.set(values[0]+","+values[1]);
                Info.set(createInfo(values[2],values[3],values[4]));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }

            context.write(ID, Info);
        }


        public Text createInfo(String HR, String fname, String lname){
            return new Text(HR+","+ fname+","+ lname);
        }


    }

    public static class HRReducer extends Reducer<Text,Text, NullWritable, Text> {


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            NullWritable out = NullWritable.get();
            String temp = "";
            int HR = 0;

            for (Text val : values){
                String[] splits =  val.toString().split(",",2);
                temp = splits[1];
                HR = HR + myParseInt(splits[0]);
            }
            context.write(out, new Text(key+","+HR+","+ temp));

        }

        public int myParseInt(String val){
            try{
                return Integer.parseInt(val);
            }catch (NumberFormatException e){
                return 0;
            }
        }
    }

    public static Job getHomeRunJob(Configuration conf, String inputPath1, String outputPath)
            throws IOException {
        Job job = Job.getInstance(conf, "Job Three");
        job.setJarByClass(Main.class);

        job.setMapperClass(HomeRun.HRMapper.class);
        job.setReducerClass(HomeRun.HRReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath1));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }
}
