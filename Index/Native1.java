
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * @author minhquang
 */
public class Native1 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private static final String STOP_SYMBOLS[] = {".", ",", "!", "?", ":", ";", "-", "\\", "/", "*", "(", ")", "&"};
        private Text word = new Text();

        private static String canonize(String str) {
            for (String stopSymbol : STOP_SYMBOLS) {
                str = str.replace(stopSymbol, "");
            }
            return str.trim();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String doc = value.toString();
            //  Split to get username and tweet            
            String[] obj = doc.split(",", 2);
            int x = canonize(obj[1].toString()).hashCode();

            StringTokenizer itr = new StringTokenizer(canonize(obj[1].toString()));
            int numword = itr.countTokens();
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new Text(obj[0] + "&" + numword + "&" + x));
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder res = new StringBuilder();
            for (Text val : values) {
                res.append(val.toString()).append("#");
            }
            context.write(key, new Text(res.toString()));
        }
    }

  

    public static void main(String[] args) throws Exception {
        // Map Reduce Job 1: Build Spiral Cluster structure
        long startTime = System.currentTimeMillis();
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Native1");
        job1.setJarByClass(Native1.class);
        job1.setNumReduceTasks(5);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        try {
            String command = "hdfs dfs -cp " + args[1] + "/part* " + "/data/input2";

            Process proc = Runtime.getRuntime().exec(command);

        } catch (Exception x) {
        }
	System.out.println("MR22222222222222222222222222222222222");


    }
}

