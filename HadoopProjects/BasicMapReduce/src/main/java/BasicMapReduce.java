import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.StringTokenizer;


public class BasicMapReduce {

    public static class BasicMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        private final static DoubleWritable runsCreated = new DoubleWritable(0);
        private Text player = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (key.get()== 0 && value.toString().contains("playerID,yearID,stint,teamID,lgID,G,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,IBB,HBP,SH,SF,GIDP")){
                    System.out.println(key+"  "+value);
                }
                else {
                    String[] values = value.toString().split(",");
                    player.set(values[0]);
                    Double OBP = calculateOBP(parseDouble(values[8]),parseDouble(values[15]),parseDouble(values[17]),parseDouble(values[18]),parseDouble(values[6]),parseDouble(values[20]),parseDouble(values[19]));
                    Double TB = calculateTB(parseDouble(values[8]),parseDouble(values[9]),parseDouble(values[10]),parseDouble(values[11]));
                    //System.out.println(OBP * TB);
                    runsCreated.set(OBP * TB);
                System.out.println(player+" "+runsCreated.get());
                    context.write(player, runsCreated);
                }
        }

        public Double calculateOBP(Double h, Double bb, Double ibb, Double hbp, Double ab, Double sf, Double sh){
            //return (h+bb+ibb+hbp);

            return (h+bb+ibb+hbp)/(ab+bb+ibb+hbp+sf+sh);
        }

        public Double calculateTB(Double h, Double twoB, Double threeB, Double hr){
            Double OneB = h - (twoB + threeB + hr);
            return OneB + 2*(twoB) + (3*threeB) + (4*hr);
        }

        public Double parseDouble(String val){
            try{
                return Double.parseDouble(val);
            }catch (NumberFormatException e){
                return 0.0;
            }
        }
    }

    public static class BasicReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
                System.out.println(key+" "+sum);
            }
            result.set(sum);
            context.write(key, result);
        }
    }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Basic Map Reduce");
            job.setJarByClass(BasicMapReduce.class);
            job.setMapperClass(BasicMapper.class);
            job.setCombinerClass(BasicReducer.class);
            job.setReducerClass(BasicReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss", Locale.US).format(new Timestamp(System.currentTimeMillis()));
            //FileInputFormat.addInputPath(job, new Path("./../BasicMapReduce/src/main/resources/Input"));
            //FileOutputFormat.setOutputPath(job, new Path("./../BasicMapReduce/src/main/resources/Output"+"-"+timeStamp));
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }



}
