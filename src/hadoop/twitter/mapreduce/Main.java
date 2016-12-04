/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import hadoop.twitter.mapreduce.Preprocess.*;
import hadoop.twitter.mapreduce.PageRank.*;
import hadoop.twitter.mapreduce.Ranking.RankingMapper;
import hadoop.twitter.mapreduce.Ranking.RankingReducer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
    
    private static Configuration conf = new Configuration();
      
    public static String userPath(String path) throws Exception {
        return "/user/feryandi/" + path;
    }
    
    public static void deleteFolder(String path) throws Exception {
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(path), true);
    }
    
    public static void pageRank(String name, String input, String output) {
        try {
            deleteFolder(output);
            Job job = Job.getInstance(conf, "(feryandi) " + name);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(RankMapper.class);
            job.setReducerClass(RankReducer.class);
            //job.setNumReduceTasks(4);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));         
            while(job.waitForCompletion(true) ? false : true) {}
            //deleteFolder(input);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void preProcess(String name, String input, String output) {
        try {
            deleteFolder(output);
            Job job = Job.getInstance(conf, "(feryandi) " + name);
            job.setJarByClass(Preprocess.class);
            job.setMapperClass(UserMapper.class);
            job.setCombinerClass(UserReducer.class);
            job.setReducerClass(UserReducer.class);
            //job.setNumReduceTasks(8);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));            
            while(job.waitForCompletion(true) ? false : true) {}
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
    public static void ranking(String name, String input, String output) {
        try {
            deleteFolder(output);
            Job job = Job.getInstance(conf, "(feryandi) " + name);
            job.setJarByClass(Ranking.class);
            job.setMapperClass(RankingMapper.class);
            job.setCombinerClass(RankingReducer.class);
            job.setReducerClass(RankingReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(UserWritable.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));            
            while(job.waitForCompletion(true) ? false : true) {}
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
    
    public static void main(String[] args) {
        String raw_data = args[0];
        String output_data = args[1];
        try {
            deleteFolder(output_data);
            
            preProcess("PreProcess", raw_data, userPath("out_preprocess"));
            pageRank("PageRank #1", userPath("out_preprocess"), userPath("out_pagerank_1"));
            pageRank("PageRank #2", userPath("out_pagerank_1"), userPath("out_pagerank_2"));
            pageRank("PageRank #3", userPath("out_pagerank_2"), userPath("out_pagerank_3"));
            ranking("Ranking", userPath("out_pagerank_3"), output_data);
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
