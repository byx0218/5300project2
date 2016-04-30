package util;

import java.util.Map;
import java.util.TreeMap;

public class Const {
    
    /** Some constant integer values. */
    public static final int N = 685230;
    public static final int BLOCKS = 68;
    public static final int AMP = 10000;
    public static final int ITERATIONS = 5;
    
    public static final double D = 0.85;
    public static final double THRESHOLD = 0.001;
    
    /** Regular expression strings. */
    public static final String EMPTY = "";
    public static final String SPACE = " ";
    public static final String DELIMITER = "\\s+";
    
    /** String prefixes. */
    public static final String PREFIX_PR = "pr";
    public static final String PREFIX_BE = "be";
    public static final String PREFIX_BC = "bc";
    public static final String PREFIX_DST_IDS = "dstIds";
    
    /** Block ID boundary list. */
    public static final long[] bound =
            new long[] {10328, 20373, 30629, 40645, 50462, 60841, 70591, 80118,
                    90497, 100501, 110567, 120945, 130999, 140574, 150953, 161332,
                    171154, 181514, 191625, 202004, 212383, 222762, 232593, 242878,
                    252938, 263149, 273210, 283473, 293255, 303043, 313370, 323522,
                    333883, 343663, 353645, 363929, 374236, 384554, 394929, 404712,
                    414617, 424747, 434707, 444489, 454285, 464398, 474196, 484050,
                    493968, 503752, 514131, 524510, 534709, 545088, 555467, 565846,
                    576225, 586604, 596585, 606367, 616148, 626448, 636240, 646022,
                    655804, 665666, 675448, 685230};

    
    /**
     * Given a Node ID, return the corresponding Block ID.
     * @param nodeId Node ID
     * @return Block ID of the given Node ID
     */
    public static long blockIdOfNode(long nodeId) {
        int lo = 0;
        int hi = 67;
        
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            
            if (nodeId < bound[mid] && (mid == 0 || bound[mid - 1] <= nodeId)) {
                return (long) mid;
            }
            
            if (nodeId < bound[mid]) {
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
            
            mid = (lo + hi) / 2;
        }
        
        return (long) lo;
    }
    
    
    public static Map<Long, Double> twoLowestNodesEachBlock() {
        Map<Long, Double> ret = new TreeMap<Long, Double>();
        double initPR = 0.0;
        ret.put(0L, initPR);
        ret.put(1L, initPR);
        
        for (int i = 0; i < 67; i ++) {
            ret.put(bound[i], initPR);
            ret.put(bound[i] + 1, initPR);
        }
        
        return ret;
    }
}
