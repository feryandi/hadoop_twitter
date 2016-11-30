/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoop.twitter.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author feryandi
 */
public class UserWritable implements Writable {
    private DoubleWritable pageRank;
    private Text following; 
    
    public UserWritable() {
        pageRank = new DoubleWritable(1);
        following = new Text();
    }
    
    public DoubleWritable getPageRank() {
        return pageRank;
    }

    public void setPageRank(DoubleWritable pageRank) {
        this.pageRank = pageRank;
    }

    public Text getFollowing() {
        return following;
    }
    
    public void set(Double pageRank, Text following) {        
        this.pageRank = new DoubleWritable(pageRank);
        this.following = following;
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        pageRank.write(d);
        following.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        pageRank.readFields(di);
        following.readFields(di);
    }
    
    @Override
    public String toString() {
        return pageRank + "\t" + following.toString();
    }
}
