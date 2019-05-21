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

public class MapReduceJob3 {

    public static class MapReduceJob3Map extends Mapper<Object, Text, Text, Text> {

        private int querynum;
        private String querykshingle;
        private double epsilon;
	@Override
        protected void setup(Context context) throws IOException, InterruptedException {
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
            String[] res1 = clusterInf.split("	");
            String[] docs = res1[1].split("#");
            for (String doc : docs) {
                String[] _doc = doc.split("@");
                String URLdoc = _doc[0];
                double similarityScore = kHSimHelper.calculateSim(_doc[2], querykshingle);
                if (similarityScore >= epsilon) {
                    context.write(new Text(URLdoc),
                            new Text(String.valueOf(similarityScore)));
                }
            }

        }

    }

    public static class MapReduceJob3Reduce extends Reducer<Text, Text, Text, Text> {
	@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text similarityScore : values) {
                context.write(key, similarityScore);
            }
        }
    }
}
