/**
 * Multipart donwloader by Sam46.
 * https://github.com/sam46
 */
import java.io.File;

import org.apache.commons.cli.*;

public class Tachyon {
	public static void main(String[] args) throws Exception {
		String outPath = null;
		 String url = null;
		 int con = Settings.DEFUALT_MAX_CONNECTIONS;
		 // String url = ("https://storage.googleapis.com/vimeo-test/work-at-vimeo-2.mp4");
//		 String url = ("https://66skyfiregce-vimeo.akamaized.net/exp=1518823952~acl=%2F130695665%2F%2A~hmac=1765dfb0a222a76006bfcc16b84a74c1db5a8f987db6661c22d1ee16bb0a9544/130695665/sep/audio/379792298/chop/segment-2.m4s");
//		 String url = ("https://github.com/sam46/Tachyon/blob/master/app5.avi");
//		 String url = ("http://192.168.232.1/app5.avi");
//		String url = ("http://192.168.232.1/work-at-vimeo-2.mp4");
//		 String url = ("http://192.168.232.1/nonexistent.mp4");
		
		Options options = new Options();
		HelpFormatter formatter = new HelpFormatter();
		options.addOption("url", true, "url to download");
		options.addOption("o", true, "path to output file");
		options.addOption("c", true, "max number of parallel connections > 0 (default=4)");
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse( options, args);
		} catch (Exception e) {
			formatter.printHelp( "Tachyon", options );
			return;
		}
		if(cmd.hasOption("url")) {
			url = cmd.getOptionValue("url");
//			System.out.println(url);
		}
		else {
			formatter.printHelp( "Tachyon", options );
			return;
		}
		if(cmd.hasOption("c")) {
			try {
				con = Integer.parseInt(cmd.getOptionValue("c"));
			} catch (Exception e) {
				System.out.println("invalid number of connections");
				formatter.printHelp( "Tachyon", options );
				return;
			}
		}
		
		if(cmd.hasOption("o")) {
			outPath = cmd.getOptionValue("o");
			System.out.println(outPath);
			File file = new File(outPath);
			if (! (file.getParentFile().isDirectory() && file.getParentFile().exists()) ) {
				System.out.println("invalid path");
				return;
			}
			new TachyonDownload(url, outPath, con);
			System.out.println("Exiting..");
		}
		else {
			formatter.printHelp( "Tachyon", options );
			return;
		}
		

	}
}
