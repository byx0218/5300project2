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

    private static final String DELIMITER = "\\s+";

    @Override
    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output,
            Reporter reporter)
            throws IOException {
        
        // value format will be:
        // "srcId 1.0 dstId1 dstId2 ... dtsIdn"
        String line = value.toString().trim();
        String[] node = line.split(DELIMITER, 3);
        String srcId = node[0];
        double pr = Double.parseDouble(node[1]);
        String dstIds = "";
        
        // this node has out going edges
        if (node.length == 3) {
            dstIds = node[2].trim();
            String[] dstIdArr = dstIds.split(DELIMITER);
            int outDegree = dstIdArr.length;
            
            
            for (String dstId : dstIdArr) {
                Text outKey = new Text(dstId);
                Text outValue = new Text(Double.toString(pr / outDegree));
                output.collect(outKey, outValue);
            }
        }
        
        Text outKey = new Text(srcId);
        Text outDstIds = new Text("dstIds " + dstIds);
        Text outOldPr = new Text("pr " + node[1]);
        output.collect(outKey, outDstIds);
        output.collect(outKey, outOldPr);
    }

}
