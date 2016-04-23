package pagerank;

import java.util.ArrayList;
import java.util.List;

public class Node {
	
	/** ID of this node. */
	private String nodeId;
	
	/** Page rank of this node. */
	private float pageRank;
	
	/** Neighbors (destinations) of this node. */
	private List<String> dest;
	
	public Node(String id) {
		this(id, 0.0f);
	}
	
	public Node(String id, float pr) {
		nodeId = id;
		pageRank = pr;
		dest = new ArrayList<>();
	}
	
	public String getNodeId() {
		return nodeId;
	}
	
	public float getPageRank() {
		return pageRank;
	}
	
	public void setPageRank(float pr) {
		pageRank = pr;
	}
	
	public void addDestination(String id) {
		dest.add(id);
	}
	
	public List<String> getDestinationList() {
		return dest;
	}
}
