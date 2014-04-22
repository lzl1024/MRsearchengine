package baseline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

public class ExternalSortHelper {
	
	/**
     * 
     * Key-value pair store in the heap
     * 
     */
    public static class KVPair {
        public String url;
        public double score;
        public int fileID;

        public KVPair(String url, double score, int fileID) {
            super();
            this.url = url;
            this.score = score;
            this.fileID = fileID;
        }

		@Override
        public String toString() {
	        return "KVPair [url=" + url + ", score=" + score + ", fileID="
	                + fileID + "]";
        }
    }
	
    
    // rewrite comparator
    public static class ScorePrio implements Comparator<KVPair> {

        @Override
        public int compare(KVPair p1, KVPair p2) {
            if (p1.score < p2.score) {
                return 1;
            } else if (p1.score > p2.score) {
                return -1;
            }
            return 0;
        }

    }
	
    
    // external sort
	public static void externalSort(int total_files, String output) throws Exception {

        PriorityQueue<KVPair> heap = new PriorityQueue<KVPair>(20,
                new ScorePrio());
        ArrayList<BufferedReader> readers = new ArrayList<BufferedReader>();
        String line = null;
        PrintWriter writer = new PrintWriter(new FileWriter(output));
        
        // open files
        for (int i = 1; i <= total_files; i++) {
            // get first records
            BufferedReader reader = new BufferedReader(new FileReader(i + ".txt"));
            if ((line = reader.readLine()) != null) {
                heap.offer(parseLine(line, i-1));
            }
            readers.add(reader);
        }

        // external merge
        while (heap.size() > 0) {
            KVPair tmp = heap.poll();

            writer.println(tmp.url+ " " + tmp.score);

            // read next record in the same file
            if ((line = readers.get(tmp.fileID).readLine()) != null) {
                heap.offer(parseLine(line, tmp.fileID));
            } else {
                readers.get(tmp.fileID).close();
            }
        }
        writer.close();
        
        
        // delete files
        for (int i = 1; i <= total_files; i++) {
            // get first records
            File f = new File(i + ".txt");
            if (f.exists()) {
            	f.delete();
            }
        }
    }


	// parse line into KVPair
	private static KVPair parseLine(String line, int fileID) {
	    String[] e = line.split(" ");
	    return new KVPair(e[0], Double.parseDouble(e[1]), fileID);
    }
    
}
