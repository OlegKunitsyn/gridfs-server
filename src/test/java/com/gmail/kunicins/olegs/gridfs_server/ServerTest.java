package com.gmail.kunicins.olegs.gridfs_server;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ConnectException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

public class ServerTest {

	private Thread thread;
	private static String GRIDFS_HOST = "192.168.56.2";
	private static int GRIDFS_PORT = 27017;
	private static String GRIDFS_DB = "test";
	private static int HTTP_PORT = 8080;
	private static final int CONCURRENT_CONNECTIONS = 200;
	
	private static GridFS gridFs;
	
	private enum TestFile {
	    TINY("539a1e21029680157c580e1b", "tiny.gif", "ad4b0f606e0f8465bc4c4c170b37e1a3", "image/gif"),
	    SMALL("539af86a0296801108b87ada", "small.bmp", "b5130669d5dfb96da4b4102ee3f1e8fd", "image/bmp"),
	    MID("539af9810296801108b87adc", "mid.bmp", "bc6305a1d0e92ab616f8dbbd5228235f", "image/bmp");
	
		public String id;
		public String name;
		public String md5;
		public String contentType;
	    
	    private TestFile(String id, String name, String md5, String contentType) {
	    	this.id = id;
	        this.name = name;
	        this.md5 = md5;
	        this.contentType = contentType;
	    }
	}
	
	class DownloadThread extends Thread {
		private Download job;
		
		public DownloadThread(Download job) {
			super(job);
			this.job = job;
		}
		
		public String getMD5() {
			return this.job.md5;
		}
		
		public int getSize() {
			return this.job.size;
		}
	}
	
	class Download implements Runnable{
		private String mongoId;
		public String md5;
		public int size;
		public Exception e;
		
		public Download(String mongoId) {
			this.mongoId = mongoId;
		}
		
		public void run() {
			try {
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				URL url;
				InputStream in;
				try {
					url = new URL("http://127.0.0.1:" + HTTP_PORT + this.mongoId);
					in = url.openStream();
				} catch (ConnectException e) {
					System.err.println(e.getMessage());
					return;
				}
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
				this.size = out.size();
				this.md5 = new BigInteger(1, md.digest()).toString(16);
			} catch (Exception e) {
				this.e = e;
			}
		}
	}
	
	@BeforeClass
	public static void constructTestEnvironment() throws IOException {
		gridFs = new GridFS(new MongoClient(GRIDFS_HOST, GRIDFS_PORT).getDB(GRIDFS_DB));
		for(TestFile file : TestFile.values()) {
			GridFSInputFile gfsFile = gridFs.createFile(new File("src/test/resources/" + file.name));
			gfsFile.setFilename(file.name);
			gfsFile.setContentType(file.contentType);
			gfsFile.setId(new ObjectId(file.id));
			gfsFile.save();
		}
	}
	
	@AfterClass
	public static void destructTestEnvironment() {
		for(TestFile file : TestFile.values()) {
			gridFs.remove(new ObjectId(file.id));
		}
	}
	
	@Before
	public void setUp() throws InterruptedException {
		this.thread = new Thread(new Server(GRIDFS_HOST, GRIDFS_PORT, GRIDFS_DB, HTTP_PORT, CONCURRENT_CONNECTIONS));
		this.thread.start();
		Thread.sleep(100);
	}
	
	@After
	public void tearDown() {
		this.thread.interrupt();
	}
	
	@Test
	public void testNotFoundIncorrectMongoId() throws IOException {
		Download file = new Download("/test");
		file.run();
		Assert.assertTrue(file.e instanceof FileNotFoundException);
	}

	@Test
	public void testNotFoundEmptyMongoId() throws IOException {
		Download file = new Download("");
		file.run();
		Assert.assertTrue(file.e instanceof FileNotFoundException);
	}
	
	@Test
	public void testNotFoundUnkownMongoId() {
		Download file = new Download("/000000000000000000000000");
		file.run();
		Assert.assertTrue(file.e instanceof FileNotFoundException);
	}

	@Test
	public void testTinyImage() {
		Download file = new Download("/" + TestFile.TINY.id);
		file.run();
		Assert.assertEquals(TestFile.TINY.md5, file.md5);
	}
	
	@Test
	public void testSmallImage() {
		Download file = new Download("/" + TestFile.SMALL.id);
		file.run();
		Assert.assertEquals(TestFile.SMALL.md5, file.md5);
	}
		
	@Test
	public void testMidImage() {
		Download file = new Download("/" + TestFile.MID.id);
		file.run();
		Assert.assertEquals(TestFile.MID.md5, file.md5);
	}
	
	@Test
	public void testConcurrentDownload() throws InterruptedException {
		int concurrentConnections = 100;
		
		// initialize threads
		DownloadThread thread;
		ArrayList<DownloadThread> pool = new ArrayList<DownloadThread>();
		for (int i = 0; i < concurrentConnections; i++) {
			thread = new DownloadThread(new Download("/" + TestFile.MID.id)); 
			pool.add(thread);
			thread.start();
		}

		// run
		for (int i = 0; i < concurrentConnections; i++) {
			thread = pool.get(i); 
			thread.join();
			Assert.assertEquals(TestFile.MID.md5, thread.getMD5());
		}
	}
	
	@Test
	public void testBenchmarkMbps() throws InterruptedException {
		long testTime = 5000; // millis
		double shallBeMoreThan = 70.0; // Mbit/s
		
		long startTimestamp = new Date().getTime(), duration, received = 0;
		do {
			Download file = new Download("/" + TestFile.MID.id);
			file.run();
			Assert.assertEquals(TestFile.MID.md5, file.md5);
			received += 786486;
			duration = new Date().getTime() - startTimestamp;
		} while (duration < testTime);
		double mbps = ((1000 * received / duration) / 1_000_000) * 8;
		
		System.out.println("Benchmark, Mbps: " + mbps);
		Assert.assertTrue((Double.compare(mbps, shallBeMoreThan) > 0));
	}
}
