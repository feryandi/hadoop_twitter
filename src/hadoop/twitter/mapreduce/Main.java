/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import hadoop.twitter.mapreduce.Preprocess.*;
import hadoop.twitter.mapreduce.PageRank.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author feryandi
 */
public class Main {    
    
    public static void pageRank(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "(feryandi) PageRank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(RankMapper.class);
            job.setReducerClass(RankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void preProcess(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "(feryandi) Preprocess");
            job.setJarByClass(Preprocess.class);
            job.setMapperClass(UserMapper.class);
            job.setCombinerClass(UserReducer.class);
            job.setReducerClass(UserReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
    public static void main(String[] args) {
    }
    
}
