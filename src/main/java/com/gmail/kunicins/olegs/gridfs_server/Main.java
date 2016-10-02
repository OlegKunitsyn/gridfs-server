package com.gmail.kunicins.olegs.gridfs_server;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class Main {
	private static final String VERSION = "0.2";
	private static final String WELCOME = ""
			+ "-- ----------------------------------------------------------------------------\n"
			+ "-- GridFS Server v" + VERSION + " by Oleg Kunitsyn\n"
			+ "-- Licensed under Creative Commons Attribution (CC-BY)\n"
			+ "-- ----------------------------------------------------------------------------";
	private static final String USAGE = ""
			+ "-- Usage:	gridfs-server.jar <configFile>\n"
			+ "-- ----------------------------------------------------------------------------";
	
	public static void main(String args[]) {
		// fetch configuration gridfsFile
		System.out.println(WELCOME);
		if (args.length == 0) {
			System.out.println(USAGE);
			System.exit(0);
		}
		Properties config = new Properties();
		InputStream file = null;
		try {
			file = new FileInputStream(args[0]);
			config.load(file);
		} catch (Exception e) {
			System.err.println("Configuration error: " + e.getMessage());
			try {
				if (null != file)
					file.close();
			} catch (Exception ex) {
			}
			finally {
				System.exit(1);
			}
		}
		
		// start server
		Server server = new Server(
			config.getProperty("gridfs.host", "127.0.0.1"),
			Integer.parseInt(config.getProperty("gridfs.port", "27017")),
			config.getProperty("gridfs.database"),
			Integer.parseInt(config.getProperty("http.port", "80")),
			Integer.parseInt(config.getProperty("http.connections", "1000"))
		);
		new Thread(server).start();
	}
}