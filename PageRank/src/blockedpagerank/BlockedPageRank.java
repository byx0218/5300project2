package blockedpagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
        ERROR, BLOCK_ITER
    }
    
    private static List<Double> blockIters = new ArrayList<>();
    private static List<Double> residuals = new ArrayList<>();
    protected static Map<Long, Double> twoLowestNodesEachBlock = util.Const.twoLowestNodesEachBlock();
    
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input_path> <output_path>");
            System.exit(1);
        }
        
        String inputPath = args[0];
        String outputPath = args[1];
        
        double residual = Double.MAX_VALUE;
        int iter = 0;
        
        while (residual > util.Const.THRESHOLD) {
            if (iter > 0) {
                inputPath = outputPath + "/block_output" + iter;
            }
            
            residual = runBlockedPageRank(inputPath, outputPath + "/block_output" + (iter + 1));
            residuals.add(residual);
            iter ++;
        }
        
        System.out.println();
        
        for (int i = 0; i < residuals.size(); i ++) {
            System.out.println("Iteration " + i + " avg error " + residuals.get(i));
            System.out.println("Avg iterations of block " + blockIters.get(i));
            System.out.println();
        }
        
        Iterator<Long> nodeIter = twoLowestNodesEachBlock.keySet().iterator();
        
        for (int i = 0; i < util.Const.BLOCKS; i ++) {
            long nodeId1 = nodeIter.next();
            long nodeId2 = nodeIter.next();
            
            System.out.println("Block ID " + i);
            System.out.println("   Node ID " + nodeId1 + ": " + twoLowestNodesEachBlock.get(nodeId1));
            System.out.println("   Node ID " + nodeId2 + ": " + twoLowestNodesEachBlock.get(nodeId2));
            System.out.println();
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
        long iter = job.getCounters().findCounter(BlockedPageRank.Residual.BLOCK_ITER).getValue();
        blockIters.add(((double) iter) / util.Const.BLOCKS);
        return residual / (1.0 * util.Const.AMP) / util.Const.BLOCKS;
    }
    
}
