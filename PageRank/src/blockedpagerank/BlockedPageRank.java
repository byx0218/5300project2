package blockedpagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class BlockedPageRank {

    protected static enum Residual {
        ERROR
    }
    
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input_path> <output_path>");
            System.exit(1);
        }
        
        String inputPath = args[0];
        String outputPath = args[1];
        
        List<Double> residuals = new ArrayList<>();
        double residual = Double.MAX_VALUE;
        int iter = 0;
        
        while (residual > util.Const.THRESHOLD) {
            if (iter > 0) {
                inputPath = outputPath + "/block_output" + iter;
            }
            
            residual = runBlockedPageRank(inputPath, outputPath + "/block_output" + (iter + 1));
            residuals.add(residual);
            System.out.println("******* Iteration " + iter + " avg error " + residual);
            iter ++;
        }
        
        for (int i = 0; i < residuals.size(); i ++) {
            System.out.println("Iteration " + i + " avg error " + residuals.get(i));
        }
    }
    
    
    public static double runBlockedPageRank(String inputPath, String outputPath)
            throws IOException {
        JobConf conf = new JobConf(BlockedPageRank.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(BlockedPageRankMapper.class);
        conf.setReducerClass(BlockedPageRankReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        RunningJob job = JobClient.runJob(conf);
        long residual = job.getCounters().findCounter(BlockedPageRank.Residual.ERROR).getValue();
        return residual / (1.0 * util.Const.AMP) / util.Const.BLOCKS;
        
    }
    
}
