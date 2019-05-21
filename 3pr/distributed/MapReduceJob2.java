/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.lang.Math;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MapReduceJob2 {

    public static class MapReduceJob2Mapper extends Mapper<Object, Text, Text, Text> {

        public int querynum;
        public String querykshingle;
        public double epsilon;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String[] query = conf.getStrings("query");
            querynum = query[0].split(";").length;
            querykshingle = query[0];
            epsilon = Double.parseDouble(query[1]);
            
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //Reading path from the value passed above in map where the file is present.		
            String clusterInf = value.toString();
            String[] res1 = clusterInf.split("\t");
            String[] docs = res1[1].split("#");
            for (String doc : docs) {
                String[] _doc = doc.split("@");
                String URLdoc = _doc[0];
                double similarityScore = kHSimHelper.calculateSim(_doc[2], querykshingle);
                //System.out.print(URLdoc + "-------------------" +similarityScore);
                if (similarityScore >= epsilon) {
                    context.write(new Text(URLdoc),
                            new Text(String.valueOf(similarityScore)));
                }
            }

        }

    }

    public static class MapReduceJob2Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text similarityScore : values) {
                context.write(key, similarityScore);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // read file midProcessRes 
        String[] res = new String[5];
	kHSimHelper.PATH = args[4];
        try {
            File file = new File(args[4] +"/res/midProcessRes");//should get hdfs FIX data/src/midProcessRes
            Scanner sc = new Scanner(file);
            int i = 0;
            while (sc.hasNextLine()) {
                res[i] = sc.nextLine();
                i++;
            }
        } catch (FileNotFoundException e) {
        }

        // Now go to MR2 with max epsilon
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduceJob2");
                System.out.print(res[0] + "\n" +res[1]);
        job.getConfiguration().setStrings("query", res[0], res[1]);

        job.setJarByClass(MapReduceJob2.class);
        job.setMapperClass(MapReduceJob2Mapper.class);
        job.setReducerClass(MapReduceJob2Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        //download file result2 from MR2
        try {
            String command = "hdfs dfs -get " + args[1] + "/part* " + args[4] +"/res/result2";

            Process proc = Runtime.getRuntime().exec(command);
            BufferedReader reader
                    = new BufferedReader(new InputStreamReader(proc.getInputStream()));

            String line = "";
            while ((line = reader.readLine()) != null) {
                System.out.print(line + "\n");
            }

        } catch (Exception x) {
        }

        // read file result2 from MR2 and count and check K . 
        int K = 0;
        try {
            File file = new File(args[4] +"/res/result2");//FIX Should use all print about for count and check
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                sc.nextLine();
                K++;
            }
        } catch (FileNotFoundException e) {
        }
        if (K < kHSimHelper.K) {
            //Start MR3 with min epsilon 
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "MapReduceJob3");
            job2.getConfiguration().setStrings("query", res[0], res[3]);

            job2.setJarByClass(MapReduceJob3.class);
            job2.setMapperClass(MapReduceJob3.MapReduceJob3Map.class);
            job2.setReducerClass(MapReduceJob3.MapReduceJob3Reduce.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));
            job2.waitForCompletion(true);

            try {
                String command = "hdfs dfs -get " + args[3] + "/part* " + args[4] +"/res/result3";

                Process proc = Runtime.getRuntime().exec(command);
                BufferedReader reader
                        = new BufferedReader(new InputStreamReader(proc.getInputStream()));

                String line = "";	
                while ((line = reader.readLine()) != null) {
                    System.out.print(line + "\n");
                }

            } catch (Exception x) {
            }
        }
        //read file result 2 and 3

        ArrayList<ClusterItem> sortKItems = new ArrayList<>();        //sort...

        try {
            File file = new File(args[4] +"/res/result2");//should get hdfs FIX data/src/midProcessRes
            Scanner sc = new Scanner(file);

            while (sc.hasNextLine()) {
                ClusterItem item = new ClusterItem();
                String[] temp = sc.nextLine().split("\t");
                double SimilarityScore = Double.parseDouble(temp[1]);
                item.setSimilarityScore(SimilarityScore);
                item.setUrl(temp[0]);
                sortKItems.add(item);
            }

        } catch (FileNotFoundException e) {
        }

        try {
            File file = new File(args[4] +"/res/result3");//should get hdfs FIX data/src/midProcessRes
            Scanner sc = new Scanner(file);
            while (sc.hasNextLine()) {
                ClusterItem item = new ClusterItem();
                String temp1 = sc.nextLine();
                String[] temp = temp1.split("  ");
                item.setSimilarityScore(Double.parseDouble(temp[1]));
                item.setUrl(temp[0]);
                sortKItems.add(item);
            }
        } catch (FileNotFoundException e) {
        }

        Collections.sort(sortKItems);
        for (int i = 0; i < kHSimHelper.K; i++) {
            System.out.println(sortKItems.get(sortKItems.size()- i -1).toString());
        }

    }
}

