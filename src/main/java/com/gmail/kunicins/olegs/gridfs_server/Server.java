/*
 * Licensed under free CC-BY license.
 * See Creative Commons Attribution Alone for more information
 */

package com.gmail.kunicins.olegs.gridfs_server;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * The class creates asynchronous thread-safe HTTP server bridged to MongoDB
 * GridFS. Request GridFS objects by MongoID i.e.
 * http://127.0.0.1/52d55e9dc469883d09b6b494
 * http://127.0.0.1:8888/52d55e9dc469883d09b6b494
 * 
 * Returned HTTP status codes: 200 OK 404 File Not found 400 Bad Request
 * 
 * @author Oleg Kunitsyn
 */
public class Server implements Runnable {
	private static final String HTTP_SERVER = "gridfs-server";
	private static final String HTTP_FOUND = "HTTP/1.1 200 OK";
	private static final String HTTP_NOT_FOUND = "HTTP/1.1 404 Not Found";
	private static final String HTTP_BAD_REQUEST = "HTTP/1.1 400 Bad Request";
	private static final int WRITE_BUFFER_SIZE = 1024;
	private static final int READ_BUFFER_SIZE = 255;

	private static class Attachment {
		InputStream file;
	}

	private Selector selector;
	private ServerSocketChannel server;
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
			MongoClient mongo = new MongoClient(gridFSHost, gridFSPort);
			this.gridFs = new GridFS(mongo.getDB(gridFSDB));
			
			// HTTP initialization
			this.selector = SelectorProvider.provider().openSelector();
			this.server = ServerSocketChannel.open();
			this.server.configureBlocking(false);
			this.server.socket().bind(new InetSocketAddress(httpPort));
			this.server.register(this.selector, server.validOps());

			System.out.println("Listening on *:" + httpPort + ", uses GridFS '" + gridFSDB + "' on " + gridFSHost + ":" + gridFSPort);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Accepted
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void accept(SelectionKey key) throws IOException {
		SocketChannel newChannel = ((ServerSocketChannel) key.channel()).accept();
		newChannel.configureBlocking(false);
		newChannel.register(key.selector(), SelectionKey.OP_READ);
	}

	/**
	 * Read request
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void read(SelectionKey key) throws IOException {
		key.interestOps(0);
		SocketChannel channel = ((SocketChannel) key.channel());
		Attachment attachment = ((Attachment) key.attachment());
		ByteBuffer echoBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
		if (attachment == null) {
			key.attach(attachment = new Attachment());
		}
		if (channel.read(echoBuffer) < 1) {
			close(key);
			return;
		}

		// Parse request
		StringTokenizer tokenizer = new StringTokenizer(new String(echoBuffer.array(), "UTF-8"));
		echoBuffer = null;
		String httpMethod = tokenizer.nextToken().toUpperCase();
		String fileName = tokenizer.nextToken().substring(1);

		System.out.println("Requested file: " + fileName);

		if (!httpMethod.equals("GET")) {
			writeHeader(key, HTTP_BAD_REQUEST);
			writeHeader(key, "Server: " + HTTP_SERVER);
			writeHeader(key, "Connection: close");
			writeHeader(key, "");
			key.interestOps(SelectionKey.OP_WRITE);
			return;
		}

		try {
			ObjectId id = new ObjectId(fileName);
			GridFSDBFile file = gridFs.findOne(id);
			if (file == null) {
				throw new IllegalArgumentException();
			}
			attachment.file = file.getInputStream();
			writeHeader(key, HTTP_FOUND);
			writeHeader(key, "Server: " + HTTP_SERVER);
			writeHeader(key, "Connection: close");
			writeHeader(key, "Date: " + file.getUploadDate().toString());
			writeHeader(key, "Content-Disposition: attachment; filename=\"" + file.getFilename() + "\"");
			writeHeader(key, "Content-Transfer-Encoding: binary");
			writeHeader(key, "Expires: 0");
			writeHeader(key, "Cache-Control: must-revalidate, post-check=0, pre-check=0");
			writeHeader(key, "Pragma: public");
			writeHeader(key, "Content-Length: " + file.getLength());
			writeHeader(key, "");
		} catch (IllegalArgumentException ex) {
			writeHeader(key, HTTP_NOT_FOUND);
			writeHeader(key, "Server: " + HTTP_SERVER);
			writeHeader(key, "Connection: close");
			writeHeader(key, "");
		}
		key.interestOps(SelectionKey.OP_WRITE);
	}

	/**
	 * Connected
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void connect(SelectionKey key) throws IOException {
		key.interestOps(0);
		SocketChannel channel = ((SocketChannel) key.channel());
		channel.finishConnect();
	}

	/**
	 * Write header
	 * 
	 * @param key
	 * @param line
	 * @throws IOException
	 */
	private void writeHeader(SelectionKey key, String line) throws IOException {
		SocketChannel channel = ((SocketChannel) key.channel());
		channel.write(ByteBuffer.wrap((line + "\r\n").getBytes()));
	}

	/**
	 * Write response
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void write(SelectionKey key) throws IOException {
		key.interestOps(key.interestOps() ^ SelectionKey.OP_WRITE);
		SocketChannel channel = ((SocketChannel) key.channel());
		Attachment attachment = ((Attachment) key.attachment());
		if (attachment.file != null) {
			byte[] echoBuffer = new byte[WRITE_BUFFER_SIZE];
			int bytesRead = attachment.file.read(echoBuffer);
			if (bytesRead > 0) {
				channel.write(ByteBuffer.wrap(echoBuffer));
			} else {
				attachment.file.close();
				attachment.file = null;
			}
			echoBuffer = null;
			key.interestOps(SelectionKey.OP_WRITE);
		} else {
			close(key);
			return;
		}
	}

	/**
	 * Close connection
	 * 
	 * @param key
	 * @throws IOException
	 */
	private void close(SelectionKey key) throws IOException {
		key.cancel();
		key.channel().close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		try {
			while (!Thread.currentThread().isInterrupted()) {
				this.selector.select();
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while (it.hasNext()) {
					SelectionKey key = it.next();
					it.remove();
					if (key.isValid()) {
						try {
							if (key.isAcceptable()) {
								accept(key);
							} else if (key.isConnectable()) {
								connect(key);
							} else if (key.isReadable()) {
								read(key);
							} else if (key.isWritable()) {
								write(key);
							}
						} catch (IOException e) {
							e.printStackTrace();
							close(key);
						}
					}
				}
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			this.server.close();
			this.selector.close();
		} catch (IOException e) {}

		System.out.println("Server stopped");
	}
}
