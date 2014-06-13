package com.gmail.kunicins.olegs.gridfs_server;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;

import junit.framework.TestCase;

public class ServerTest extends TestCase {

	protected Thread thread;
	protected static String GRIDFS_HOST = "192.168.56.2";
	protected static int GRIDFS_PORT = 27017;
	protected static String GRIDFS_DB = "test";
	protected static int HTTP_PORT = 8080;

	class DownloadThread extends Thread {
		private Download job;
		
		public DownloadThread(Download job) {
			super(job);
			this.job = job;
		}
		
		public String getMD5() {
			return this.job.md5;
		}
	}
	
	class Download implements Runnable{
		private String mongoId;
		public String md5;
		public Exception e;
		
		public Download(String mongoId) {
			this.mongoId = mongoId;
		}
		
		public void run() {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				URL url = new URL("http://127.0.0.1:" + HTTP_PORT + this.mongoId);
				InputStream in = url.openStream();
				int b;
				while (true) {
					b = in.read();
					if (b != -1) {
						out.write(b);
					} else {
						break;
					}
				}
				in.close();
				
				MessageDigest md;
				md = MessageDigest.getInstance("MD5");
				md.update(out.toByteArray());
				this.md5 = new BigInteger(1, md.digest()).toString(16);
			} catch (Exception e) {
				this.e = e;
			}
		}
	}
	
	protected void setUp() throws Exception {
		super.setUp();
		this.thread = new Thread(new Server(GRIDFS_HOST, GRIDFS_PORT, GRIDFS_DB, HTTP_PORT));
		this.thread.start();
		Thread.sleep(100);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
		this.thread.interrupt();
	}
	
	public void testNotFoundIncorrectMongoId() throws IOException {
		Download file = new Download("/test");
		file.run();
		assertTrue(file.e instanceof FileNotFoundException);
	}

	public void testNotFoundEmptyMongoId() throws IOException {
		Download file = new Download("");
		file.run();
		assertTrue(file.e instanceof FileNotFoundException);
	}
	
	public void testNotFoundUnkownMongoId() {
		Download file = new Download("/539af86a0296801108b87adc");
		file.run();
		assertTrue(file.e instanceof FileNotFoundException);
	}

	public void testTinyImage() {
		Download file = new Download("/539a1e21029680157c580e1b");
		file.run();
		assertEquals("ad4b0f606e0f8465bc4c4c170b37e1a3", file.md5);
	}
	
	public void testSmallImage() {
		Download file = new Download("/539af86a0296801108b87ada");
		file.run();
		assertEquals("b5130669d5dfb96da4b4102ee3f1e8fd", file.md5);
	}
		
	public void testMidImage() {
		Download file = new Download("/539af9810296801108b87adc");
		file.run();
		assertEquals("bc6305a1d0e92ab616f8dbbd5228235f", file.md5);
	}
	
	public void testMidText() {
		Download file = new Download("/539b14d40296801108b87ae4");
		file.run();
		assertEquals("923fa8d79849ab22361466ba0c9317be", file.md5);
	}
	
	public void testConcurrentDownload() throws InterruptedException {
		int concurrentConnections = 50;
		
		// init threads
		DownloadThread thread;
		ArrayList<DownloadThread> pool = new ArrayList<DownloadThread>();
		for (int i = 0; i < concurrentConnections; i++) {
			thread = new DownloadThread(new Download("/539af9810296801108b87adc")); 
			pool.add(thread);
			thread.start();
		}

		// run
		for (int i = 0; i < concurrentConnections; i++) {
			thread = pool.get(i); 
			thread.join();
			assertEquals("bc6305a1d0e92ab616f8dbbd5228235f", thread.getMD5());
		}
	}
}
