import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class PopularMerchantMR {
    public static class PopularMerchantMapper extends Mapper<Object, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable();

        /**
         * Set key tobe item_id, value tobe the sum of action_type.
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] recordSet = line.split(",");
            if (!recordSet[6].equals("0")) {
                k.set(recordSet[3]);
                v = new IntWritable(1);
                context.write(k,v);
            }
        }
    }

    public static class sumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable sumResult = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable action_type : values) {
                sum = sum + action_type.get();
            }
            sumResult.set(sum);
            context.write(key, sumResult);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable,Text> {
        private Text result = new Text();
        private IntWritable ct = new IntWritable();
        int count = 0;
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (count >= 100) {
                    break;
                }
                count ++;
                ct.set(count);
                String s = val.toString() + " times: " + key.toString();
                result.set(s);
                context.write(ct,result);
            }
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();
        Path tempDir = new Path(remainingArgs[1]);

        if (remainingArgs.length != 3) {
            System.err.println("Usage: PopularIterm <in> <temp> <out>");
            System.exit(2);
        }

        Job jobOne = Job.getInstance(conf, "sum up");
        jobOne.setJarByClass(PopularMerchantMR.class);
        jobOne.setMapperClass(PopularMerchantMapper.class);
        jobOne.setReducerClass(sumReducer.class);
        jobOne.setOutputKeyClass(Text.class);
        jobOne.setOutputValueClass(IntWritable.class);
        jobOne.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(jobOne, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(jobOne, tempDir);
        jobOne.waitForCompletion(true);

        Job sortJob = new Job(conf, "sort");
        FileInputFormat.addInputPath(sortJob, tempDir);
        sortJob.setInputFormatClass(SequenceFileInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);
        sortJob.setMapperClass(InverseMapper.class);
        sortJob.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(sortJob,
                new Path(remainingArgs[2]));
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setReducerClass(SortReducer.class);
        sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
        sortJob.waitForCompletion(true);
        System.exit(0);
    }



}
