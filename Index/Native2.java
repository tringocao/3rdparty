
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
import java.io.File;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.util.Comparator;

/**
 *
 * @author minhquang
 */
public class Native2 {

    public static class QueryMapper extends Mapper<Object, Text, Text, Text> {

        private BufferedReader fis;
        private Set<String> contentRead = new HashSet<String>();
        private Set<String> wordQuery = new HashSet<String>();
        private Configuration conf;
        public static String QUERY = "";

        private void parseFiles(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String line = null;
                while ((line = fis.readLine()) != null) {
                    contentRead.add(line);
                    break;

                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] filesURIs = Job.getInstance(conf).getCacheFiles();
            for (URI fileURI : filesURIs) {
                Path path = new Path(fileURI.getPath());
                String fileName = path.getName();
                parseFiles(fileName);
            }

            for (String i : contentRead) {
                QUERY = i;
                break;
            }
            QUERY = QUERY.trim();
            StringTokenizer queryitr = new StringTokenizer(QUERY);

            while (queryitr.hasMoreTokens()) {
                wordQuery.add(queryitr.nextToken());
            }
            System.out.println(wordQuery);

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String doc = value.toString();
            //  Split to get username and tweet            
            String[] obj = doc.split("\t", 2);
            if (wordQuery.contains(obj[0])) {

                String[] res2 = obj[1].split("#");
                for (String it : res2) {

                    context.write(new Text(it + "&" + wordQuery.size()), new Text(obj[0]));
                }
            }
        }
    }

    public static class SumQueryReducer extends Reducer<Text, Text, Text, IntWritable> {

        private BufferedReader fis;
        private Set<String> contentRead = new HashSet<String>();
        private Set<String> wordQuery = new HashSet<String>();
        private Configuration conf;
        public static String QUERY = "";

        private IntWritable result = new IntWritable();

        private void parseFiles(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String line = null;
                while ((line = fis.readLine()) != null) {
                    contentRead.add(line);
                    break;

                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            URI[] filesURIs = Job.getInstance(conf).getCacheFiles();
            for (URI fileURI : filesURIs) {
                Path path = new Path(fileURI.getPath());
                String fileName = path.getName();
                parseFiles(fileName);
            }

            for (String i : contentRead) {
                QUERY = i;
                break;
            }
            QUERY = QUERY.trim();
            StringTokenizer queryitr = new StringTokenizer(QUERY);

            while (queryitr.hasMoreTokens()) {
                wordQuery.add(queryitr.nextToken());
            }
            System.out.println(wordQuery);

        }

        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            HashMap<String, Integer > list = new HashMap<>();
            for(String temp:wordQuery){
                list.put(temp, 1);
            }
            for (Text val : values) {
                if (wordQuery.contains(val.toString())) {
                   if( list.get(val.toString()) == 1){
                       sum ++;
                       list.put(val.toString(),2);
                   }
                }

            }
            result.set(sum);

            context.write(key, result);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        // Map Reduce Job 1: Build Spiral Cluster structure
        long startTime = System.currentTimeMillis();

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Native2");
        job2.addCacheFile(new Path(args[0]).toUri());
        job2.setJarByClass(Native2.class);
        job2.setMapperClass(QueryMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(SumQueryReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/data/input2"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        if (!job2.waitForCompletion(true)) {
            System.out.println("ERROR completing first jobb");
            System.exit(1);
        }
    }
}

