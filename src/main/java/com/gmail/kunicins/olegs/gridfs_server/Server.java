/*
 * Licensed under free CC-BY license.
 * See Creative Commons Attribution Alone for more information
 */

package com.gmail.kunicins.olegs.gridfs_server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import org.bson.types.ObjectId;

import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * The class creates asynchronous thread-safe HTTP server bridged to MongoDB GridFS.
 * Request GridFS objects by MongoID i.e. 
 *   http://127.0.0.1/52d55e9dc469883d09b6b494
 *   http://127.0.0.1:8888/52d55e9dc469883d09b6b494
 *   
 * Returned HTTP status codes:
 *   200 OK
 *   404 File Not found
 *   400 Bad Request
 * 
 * @author Oleg Kunitsyn
 */
public class Server implements Runnable {	
	private static final String HTTP_SERVER = "gridfs-server";
	private static final String HTTP_FOUND = "HTTP/1.1 200 OK";
	private static final String HTTP_NOT_FOUND = "HTTP/1.1 404 Not Found";
	private static final String HTTP_BAD_REQUEST = "HTTP/1.1 400 Bad Request";
	
	private ConcurrentHashMap<Integer, SocketChannel> channelPool = new ConcurrentHashMap<Integer, SocketChannel>();
	private Selector selector;
	private GridFS gridFs;
	
	/**
	 * Initialize HTTP server
	 * 
	 * @param gridFSHost
	 * @param gridFSPort
	 * @param gridFSDB
	 * @param httpPort
	 */
	public Server(String gridFSHost, int gridFSPort, String gridFSDB, int httpPort) {
		try {
			// GridFS initialization
			Mongo mongoServer = new Mongo(gridFSHost, gridFSPort);
			this.gridFs = new GridFS(mongoServer.getDB(gridFSDB));
			
			// HTTP initialization
			this.selector = Selector.open();
			ServerSocketChannel server = ServerSocketChannel.open();
			server.configureBlocking(false);
			server.socket().bind(new InetSocketAddress(httpPort));
			server.register(this.selector, SelectionKey.OP_ACCEPT);
			System.out.println("Listening on *:" + httpPort + ", uses GridFS '" + gridFSDB + "' on " + gridFSHost + ":" + gridFSPort);
			
		} catch (IOException e) {
			System.err.print(e.getMessage());
			System.err.println(e.getStackTrace()[0]);
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Send HTTP header
	 * 
	 * @param channel
	 * @param line
	 * @throws IOException
	 */
	private void sendHeader(SocketChannel channel, String line) throws IOException {
		channel.write(ByteBuffer.wrap((line + "\r\n").getBytes()));
	}
	
	/**
	 * Send HTTP content
	 * 
	 * @param channel
	 * @param content
	 * @throws IOException
	 */
	private void sendContent(SocketChannel channel, byte[] content) throws IOException {
		sendHeader(channel, "");
		channel.write(ByteBuffer.wrap(content));
	}
	
	/**
	 * @param channel
	 * @throws IOException
	 */
	private void closeChannel(SocketChannel channel) throws IOException {
		this.channelPool.remove(channel.hashCode());
		channel.close();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		while (true) {
			try {
				this.selector.select();
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey key = (SelectionKey) it.next();
					if ((key.readyOps() & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
						// Accept
						SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();
						channel.configureBlocking(false);
						channel.register(this.selector, SelectionKey.OP_READ);
						channelPool.put(channel.hashCode(), channel);
						it.remove();
					} else if ((key.readyOps() & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
						// Read
						SocketChannel channel = (SocketChannel) key.channel();
						int code = 0;
						StringBuilder sb = new StringBuilder();
						ByteBuffer echoBuffer = ByteBuffer.allocate(255);
						while ((code = channel.read(echoBuffer)) > 0) {
							byte b[] = new byte[echoBuffer.position()];
							echoBuffer.flip();
							echoBuffer.get(b);
							sb.append(new String(b, "UTF-8"));
						}
						if (code == -1) {
							closeChannel(channel);
						}
						echoBuffer = null;
						
						// Parse request
						StringTokenizer tokenizer = new StringTokenizer(sb.toString());
						String httpMethod = tokenizer.nextToken().toUpperCase();
						String fileName = tokenizer.nextToken().substring(1);
						System.out.println("Requested file: " + fileName);
	
						// Write
						switch(httpMethod) {
							case "GET":
								GridFSDBFile file = null;
								try {
									ObjectId id = new ObjectId(fileName);
									file = this.gridFs.findOne(id);
								} catch (IllegalArgumentException ex) {}
								if (file != null) {
									sendHeader(channel, HTTP_FOUND);
									sendHeader(channel, "Server: " + HTTP_SERVER);
									sendHeader(channel, "Connection: close");
									sendHeader(channel, "Date: " + file.getUploadDate().toString());
									sendHeader(channel, "Content-Disposition: attachment; filename=\"" + file.getFilename() + "\"");
									sendHeader(channel, "Content-Transfer-Encoding: binary");
									sendHeader(channel, "Expires: 0");
									sendHeader(channel, "Cache-Control: must-revalidate, post-check=0, pre-check=0");
									sendHeader(channel, "Pragma: public");
									sendHeader(channel, "Content-Length: " + file.getLength());
									byte[] gridfsObject = new byte[(int) file.getLength()];
									file.getInputStream().read(gridfsObject);
									sendContent(channel, gridfsObject);
									gridfsObject = null;
									break;
								}
								sendHeader(channel, HTTP_NOT_FOUND);
								sendHeader(channel, "Server: " + HTTP_SERVER);
								sendHeader(channel, "Connection: close");
								sendContent(channel, "".getBytes());
								break;
							default:
								sendHeader(channel, HTTP_BAD_REQUEST);
								sendHeader(channel, "Server: " + HTTP_SERVER);
								sendHeader(channel, "Connection: close");
								sendContent(channel, "".getBytes());
						}
						
						closeChannel(channel);
						it.remove();
					}
				}		
			} catch (IOException e) {
				System.err.print(e.getMessage());
				System.err.println(e.getStackTrace()[0]);
				throw new RuntimeException(e);
			}
		}
	}
}
