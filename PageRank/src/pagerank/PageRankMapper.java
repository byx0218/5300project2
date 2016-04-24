package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PageRankMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

    private static final String SPACE = " ";

    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output,
            Reporter reporter)
            throws IOException {
        
        // value format will be:
        // "srcId 1.0 dstId1 dstId2 ... dtsIdn"
        String line = value.toString().trim();
        String[] node = line.split(SPACE, 3);
        double pr = Double.parseDouble(node[1]);
        
        // this node has out going edges
        if (node.length == 3) {
            String[] dstIds = node[2].trim().split(SPACE);
            int outDegree = dstIds.length;
            
            for (String dstId : dstIds) {
                Text outKey = new Text(dstId);
                Text outValue = new Text(Double.toString(pr / outDegree));
                output.collect(outKey, outValue);
            }
        }
        
    }

}
