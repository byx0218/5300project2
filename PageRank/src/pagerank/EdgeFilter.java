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
	public Map<Long, Node> processEdges() {
		Map<Long, Node> nodeTbl = new HashMap<>();
		String line = null;
		
		try {
			BufferedReader reader =
					new BufferedReader(new FileReader(EDGES));
			
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty()) {
					break;
				}
				
				String[] edge = line.split(SPACES);
				long srcId = Long.parseLong(edge[0]);
				long dstId = Long.parseLong(edge[1]);
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
		long currSrcId = -1;
		
		try {
			BufferedReader reader =
					new BufferedReader(new FileReader(EDGES));
			BufferedWriter writer =
					new BufferedWriter(new FileWriter(FILTERED, false));
			
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty()) {
					break;
				}
				
				String[] edge = line.split(SPACES);
				long srcId = Long.parseLong(edge[0]);
				long dstId = Long.parseLong(edge[1]);
				double x = Double.parseDouble(edge[2]);
				
				if (srcId != currSrcId) {
					if (currSrcId != -1) {
						writer.newLine();
						
						while (currSrcId + 1 != srcId) {
							currSrcId ++;
							writer.write(currSrcId + "");
							writer.newLine();
						}
					}
					
					writer.write(srcId + "");
					currSrcId = srcId;
				}
				
				if (REJECT_MIN <= x && x < REJECT_MAX) {
					continue;
				}
				
				writer.write(" ");
				writer.write(dstId + "");
			}
			
			reader.close();
			writer.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Processes the filtered Edges. Returns all the Nodes in a map.
	 * @return map of all Nodes
	 */
	public Map<Long, Node> processFilteredEdges() {
		Map<Long, Node> nodeTbl = new HashMap<>();
		
		try {
			String line = null;
			BufferedReader reader =
					new BufferedReader(new FileReader("filter.txt"));
			
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty())
					break;
				
				String[] edges = line.split("\\s+");
				long srcId = Long.parseLong(edges[0]);
				
				if (!nodeTbl.containsKey(srcId)) {
					nodeTbl.put(srcId, new Node(srcId));
				}
				
				Node srcNode = nodeTbl.get(srcId);
				
				for (int i = 1; i < edges.length; i ++) {
					long dstId = Long.parseLong(edges[i]);
					srcNode.addDestination(dstId);
					
					if (!nodeTbl.containsKey(dstId)) {
						nodeTbl.put(dstId, new Node(dstId));
					}
				}
			}
			
			reader.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return nodeTbl;
	}
}
