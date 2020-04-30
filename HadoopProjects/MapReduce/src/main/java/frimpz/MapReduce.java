package frimpz;

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


/**
 * Basic Map REduce Assignment
 * @Author Frimpong Boadu
 *
 */
public class MapReduce {

    /**
     * This is the Mapper class, it contains the map function  and two fields (playerID and playerInfo)
     * Each input line is split into playerID and playerInfo before it is sent to the reducer.
     */
    public static class BasicMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text playerID = new Text();
        private Text playerInfo = new Text();


        /**
         * Splits each line into two, key and value pair and sends each key value to the reducer.
         * @param key input key received by mapper
         * @param value input value received by mapper, this is split on the first occurence of a "," into <key, value > pair
         *              and sent as (Text, Text) and sent to the reducer.
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (key.get()== 0 && value.toString().contains("playerID")){}
            else {
                String[] values = value.toString().split(",", 2);
                playerID.set(values[0]);
                playerInfo.set(values[1]);
                context.write(playerID, playerInfo);
            }
        }

    }

    /**
     * Reducer class, receives output of the mapper and calculates runs created for each player.
     */
    public static class BasicReducer extends Reducer<Text,Text,Text,DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        /**
         * This function calculates the runs created for each player.
         * @param key input key to reducer
         * @param values input value to reducer.
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int h= 0,  bb= 0, ibb= 0, hbp= 0, ab= 0, sf= 0, sh= 0, twoB= 0, threeB= 0, hr= 0;

            for (Text val : values) {
                String[] info = val.toString().split(",");
                h += parseInt(7, info);
                bb += parseInt(14, info);
                ibb += parseInt(16, info);
                hbp += parseInt(17, info);
                ab += parseInt(5, info);
                sf += parseInt(19, info);
                sh += parseInt(18, info);
                twoB += parseInt(8, info);
                threeB += parseInt(9, info);
                hr += parseInt(10, info);
            }

            double OBP = calculateOBP(h, bb, ibb, hbp, ab, sf, sh);
            int TB = calculateTB(h, twoB, threeB, hr);

            result.set(OBP * TB);
            context.write(key, result);
        }

        /**
         * This function receives a bunch of parameters and calculates the OBP
         * Division by 0 may return NAN, all such values are replaced with 0
         * @return
         */
        public double calculateOBP(int h, int bb, int ibb, int hbp, int ab, int sf, int sh){
            double obp = ((double) h+bb+ibb+hbp)/(ab+bb+ibb+hbp+sf+sh);
            if(Double.isNaN(obp)){
                return 0.0;
            }
            return obp;
        }

        /**
         * This function receives a bunch of parameters and calculates the TB
         */
        public int calculateTB(int h, int twoB, int threeB, int hr){
            int OneB = h - (twoB + threeB + hr);
            return OneB + (2*twoB) + (3*threeB) + (4*hr);
        }

        /**
         * This function receives the position of a parameter and a Text split into an array.
         * Catches an ArrayIndexOutOfBoundsException and returns 0 for each field empty in the csv file.
         * Catches NUmberformatException and returns 0 for each field also empty in the csv file.
         * @param pos, position of field in csv
         * @param values slitted String
         * @return
         */
        public int parseInt(int pos, String[] values){
            try{
                String x = values[pos];
                return Integer.parseInt(x);
            }catch (NumberFormatException e){
                return 0;
            }catch (ArrayIndexOutOfBoundsException e){
                return 0;
            }
        }
    }

    /**
     * Main method of program
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Basic--Map--Reduce");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(BasicMapper.class);

        // Not  using combiner because reducer output value is double but reducer takes a text value as input.
        // Using the combiner for intermediate calculation will produce a double value as output
        // which does not match input value of reducer.

        //job.setCombinerClass(BasicReducer.class);
        job.setReducerClass(BasicReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
