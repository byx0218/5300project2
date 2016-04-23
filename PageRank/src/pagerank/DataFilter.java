package pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DataFilter {
	
	private static final String FILTERED = "filter.txt";
	private static final String EDGES = "edges.txt";
	private static final String SPACES = "\\s+";
	
	// Filter parameters for NetID xg95.
	private static final double NET_ID = 0.59;
	private static final double REJECT_MIN = 0.9 * NET_ID;
	private static final double REJECT_MAX = REJECT_MIN + 0.01;
	
	private BufferedReader reader;
	private BufferedWriter writer;
	
	public DataFilter() {
		try {
			reader = new BufferedReader(new FileReader(EDGES));
			writer = new BufferedWriter(new FileWriter(FILTERED));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void processData() {
		String line = null;

		try {
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				
				if (line.isEmpty()) {
					break;
				}
				
				String[] edge = line.split(SPACES);
				double x = Double.parseDouble(edge[2]);
				
				if (REJECT_MIN <= x && x < REJECT_MAX) {
					continue;
				}
				
				
			}
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close() {
		try {
			reader.close();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
