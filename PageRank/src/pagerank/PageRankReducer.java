package pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        double oldPR = 0.0;
        double newPR = 0.0;
        String value = null;
        String dstIds = null;
        
        while (values.hasNext()) {
            value = values.next().toString();
            
            if (value.isEmpty()) {
                break;
            }
            
            if (value.startsWith(util.Const.PREFIX_DST_IDS)) {
                dstIds = value.split(util.Const.DELIMITER, 2)[1];
                continue;
            }
            
            if (value.startsWith(util.Const.PREFIX_PR)) {
                oldPR = Double.parseDouble(value.split(util.Const.DELIMITER, 2)[1]);
                continue;
            }
            
            newPR += Double.parseDouble(value);
        }
        
        newPR *= util.Const.D;
        newPR += (1 - util.Const.D) / util.Const.N;
        
        long residual = (long) Math.floor((Math.abs(oldPR - newPR) / newPR) * util.Const.AMP);
        reporter.incrCounter(PageRank.Residual.ERROR, residual);
        
        Text outValue = new Text(Double.toString(newPR) + util.Const.SPACE + dstIds);
        output.collect(key, outValue);
    }

}
