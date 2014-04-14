package mapred.querysearch;

import mapred.util.SimpleParser;

public class Driver {
	public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        String tmpdir = parser.get("tmpdir");

    }
}
