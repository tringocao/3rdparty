/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 *
 * @author minhquang
 */
public class NaiveMapReduce {

    public static class NaiveMapReduceMapper extends Mapper<Object, Text, NullWritable, Text> {

        public static TreeMap<ClusterItem, Text> clusterMap = new TreeMap<>(new ClusterComp());
        private static String query = "I think I want to read some books but the library doesn't have them ";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] obj = line.split(",", 2);

            double similarityScore = kHSimHelper.calculateSim(
                    kHSimHelper.uniqueQuery(kHSimHelper.genShingle(query).split("@")[1]),
                    kHSimHelper.genShingle(obj[1]).split("@")[1]);

            //insert cluster item object as  key and entire row as value
            //tree map sort the records based on sim score
//            clusterMap.put(new ClusterItem(similarityScore), new Text(value));
            //  Check for duplicate
            if (!clusterMap.containsValue(new Text(String.valueOf(value + "\t" + similarityScore)))) {
                clusterMap.put(new ClusterItem(similarityScore), new Text(String.valueOf(value + "\t" + similarityScore)));
            }

            // If we have more than ten records, remove the one with the lowest sim
            // As this tree map is sorted in descending order, the one with
            // the lowest sim is the last key.
            Iterator<Entry<ClusterItem, Text>> iter = clusterMap.entrySet().iterator();
            Entry<ClusterItem, Text> entry = null;

            while (clusterMap.size() > 10) {
                entry = iter.next();
                iter.remove();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output our ten records to the reducers with a null key
            System.out.println("SIZEEEE\t" + clusterMap.size());
            for (Text t : clusterMap.values()) {
		System.out.println("AAAAA");
                context.write(NullWritable.get(), t);
            }
        }
    }

    public static class NaiveMapReduceReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        public static TreeMap<ClusterItem, Text> clusterMap = new TreeMap<>(new ClusterComp());

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
		System.out.println("BBBBB");
                context.write(key, val);
            }
        }
    }

    static class ClusterComp implements Comparator<ClusterItem> {

        @Override
        public int compare(ClusterItem e1, ClusterItem e2) {
            if (e1.getSimilarityScore() > e2.getSimilarityScore()) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NaiveMapReduce");
        job.setJarByClass(NaiveMapReduce.class);
        job.setMapperClass(NaiveMapReduceMapper.class);
//        job.setCombinerClass(NaiveMapReduceReducer.class);
        job.setReducerClass(NaiveMapReduceReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

    }

}

