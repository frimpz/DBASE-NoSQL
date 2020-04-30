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
 * This class is used to combine all the output files into one output file
 */
public class IdentityMapReduce {

    public static class IdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }


    }

    public static class IdentityReducer extends Reducer<LongWritable,Text, NullWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            NullWritable out = NullWritable.get();
            for (Text val : values){
                context.write(out, val);
            }
        }
    }

    public static Job getIdentityMapReduceJob(Configuration conf, String inputPath1, String outputPath)
            throws IOException {
        Job job = Job.getInstance(conf, "Job Five");
        job.setJarByClass(Main.class);

        job.setMapperClass(IdentityMapper.class);
        job.setReducerClass(IdentityReducer.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath1));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

}
