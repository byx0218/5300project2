package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class EdgeFilter {
	
	/** Regular expression separator string. */
	private static final String SPACES = "\\s+";
	
	/** Files to read or write. */
	private static final String FILTERED = "filter.txt";
	private static final String EDGES = "edges.txt";
	
	/** Filter parameters from NetID "xg95". */
	private static final double NET_ID = 0.59;
	private static final double REJECT_MIN = 0.9 * NET_ID;
	private static final double REJECT_MAX = REJECT_MIN + 0.01;
	
	/**
	 * Constructs an Edge filter.
	 */
	public EdgeFilter() {
		
	}
	
	/**
	 * Processes the Edges. Filter out the Edges with values within
	 * REJECT_MIN and REJECT_MAX. Returns all the Nodes in a map.
	 * @return map of all Nodes
	 */
	public Map<String, Node> processEdges() {
		Map<String, Node> nodeTbl = new HashMap<>();
		String line = null;
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(EDGES));
			
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty()) {
					break;
				}
				
				String[] edge = line.split(SPACES);
				String srcId = edge[0];
				String dstId = edge[1];
				double x = Double.parseDouble(edge[2]);
				
				if (!nodeTbl.containsKey(srcId)) {
					nodeTbl.put(srcId, new Node(srcId));
				}
				
				if (!nodeTbl.containsKey(dstId)) {
					nodeTbl.put(dstId, new Node(dstId));
				}
				
				if (REJECT_MIN <= x && x < REJECT_MAX) {
					continue;
				}
				
				nodeTbl.get(srcId).addDestination(dstId);
			}
			
			reader.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return nodeTbl;
	}
	
	/**
	 * Filters out all the Edges valued within REJECT_MIN and REJECT_MAX.
	 * Writes the valid Edges into another file.
	 */
	public void filterEdges() {
		String line = null;
		String currSrcId = null;
		StringBuilder out = null;
		int count = 0;
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(EDGES));
			BufferedWriter writer = new BufferedWriter(new FileWriter(FILTERED));
			
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty()) {
					break;
				}
				
				String[] edge = line.split(SPACES);
				String srcId = edge[0];
				String dstId = edge[1];
				double x = Double.parseDouble(edge[2]);
				
				if (!srcId.equals(currSrcId)) {
					if (out != null) {
						writer.write(out.toString());
						writer.newLine();
						count ++;
					}
					
					currSrcId = srcId;
					out = new StringBuilder(srcId);
				}
				
				if (REJECT_MIN <= x && x < REJECT_MAX) {
					continue;
				}
				
				out.append(" ").append(dstId);
			}
			
			writer.write(out.toString());
			count ++;
			reader.close();
			writer.close();
			
			System.out.println(count);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
