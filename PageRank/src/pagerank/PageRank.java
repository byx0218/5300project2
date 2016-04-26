package pagerank;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRank {
    
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
        double[] residuals = new double[util.Const.ITERATIONS];
        
        for (int i = 0; i < util.Const.ITERATIONS; i ++) {
            if (i > 0) {
                inputPath = outputPath + "/output" + i;
            }
            
            residuals[i] = runPageRank(inputPath, outputPath + "/output" + (i + 1));
        }
        
        for (int i = 0; i < util.Const.ITERATIONS; i ++) {
            System.out.println("Iteration " + i + " avg error " + residuals[i]);
        }
    }
    
    public static double runPageRank(String inputPath, String outputPath) throws IOException {
        JobConf conf = new JobConf(PageRank.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(PageRankMapper.class);
        conf.setReducerClass(PageRankReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        RunningJob job = JobClient.runJob(conf);
        long residual = job.getCounters().findCounter(PageRank.Residual.ERROR).getValue();
        return residual / (1.0 * util.Const.AMP) / util.Const.N;
    }
    
}
