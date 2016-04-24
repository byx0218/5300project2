package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class PageRank {
	
	public static void main(String[] args) {
		EdgeFilter filter = new EdgeFilter();
//		Map<String, Node> nodeTbl1 = filter.processEdges();
		
		filter.filterEdges();
		Map<String, Node> nodeTbl2 = new HashMap<>();
		
		try {
			String line = null;
			BufferedReader reader =
					new BufferedReader(new FileReader("filter.txt"));
			
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty())
					break;
				
				String[] edges = line.split("\\s+");
				
				if (!nodeTbl2.containsKey(edges[0])) {
					nodeTbl2.put(edges[0], new Node(edges[0]));
				}
				
				Node src = nodeTbl2.get(edges[0]);
				
				for (int i = 1; i < edges.length; i ++) {
					src.addDestination(edges[i]);
					
					if (!nodeTbl2.containsKey(edges[i]))
						nodeTbl2.put(edges[i], new Node(edges[i]));
				}
			}
			
			reader.close();
			
		} catch (Exception e) {
		}
		
		System.out.println(nodeTbl2.size());
	}
	
	
}
