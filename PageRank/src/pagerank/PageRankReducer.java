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

    private static final double D = 0.85;
    private static final int N = 685230;
    private static final String SPACE = " ";
    private static final String DELIMITER = "\\s+";

    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        double oldPr = 0.0;
        double newPr = 0.0;
        String value = null;
        String dstIds = null;
        
        while (values.hasNext()) {
            value = values.next().toString();
            
            if (value.isEmpty()) {
                break;
            }
            
            if (value.startsWith("dstIds ")) {
                dstIds = value.split(DELIMITER, 2)[1];
                continue;
            }
            
            if (value.startsWith("pr ")) {
                oldPr = Double.parseDouble(value.split(DELIMITER, 2)[1]);
                continue;
            }
            
            newPr += Double.parseDouble(value);
        }
        
        newPr *= D;
        newPr += (1 - D) / N;
        
        long residual = (long) Math.floor((Math.abs(oldPr - newPr) / newPr) * 10e4);
        reporter.incrCounter(PageRank.Residual.ERROR, residual);
        
        Text outValue = new Text(Double.toString(newPr) + SPACE + dstIds);
        output.collect(key, outValue);
    }

}
