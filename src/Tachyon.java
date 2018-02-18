import java.awt.Checkbox;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import javax.net.ssl.SSLException;
import javax.swing.plaf.SliderUI;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

public class Tachyon {
	private String URL;
	private long fsize; // file size in bytes. Bytes 0 to fsize-1
	// static private long chunkSize = 1024*100; // breakup the file into
	// smaller units of this size
	// static private int nChunks; // total number of chunks: size/chunkSize
	private int nConnections = 4; // concurrent connections: number of chunks
									// we'll download in parallel. Will be used
									// for both number of connections and number
									// of concurrent threads
	private long chunkSize;
	private String serverChecksum = "";
	private final CloseableHttpClient httpclient;
	
	public Tachyon(String url) throws Exception {
		this.URL = url;
		boolean acceptsRanges = false;
		// Connections pool
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(nConnections);
		cm.setDefaultMaxPerRoute(nConnections);

		httpclient = HttpClients.custom().setConnectionManager(cm)
//				.setRetryHandler(buildRetryHandle())
				.build();

		HttpGet httpGet = new HttpGet(url);
		httpGet.addHeader(HttpHeaders.RANGE,"bytes=0-1");
		CloseableHttpResponse response = null;
		DLInfo dlinfo = null;
		try {
			response = httpclient.execute(httpGet); // TODO: retry on exception (i.e timeout, etc...)
			
			int status = response.getStatusLine().getStatusCode();
			if (status == 206) { // first time try a range request
				fsize = getFileSize(response);
				acceptsRanges = true;//fsize != -1 && testAcceptRanges(response);
			}
			else if (status >= 200 && status < 300) { 
				httpGet.removeHeaders(HttpHeaders.RANGE);
				response.close();
				response = httpclient.execute(httpGet);
				fsize = getFileSize(response);
			} else {
				throw new Exception("TODO. Got "+status); // TODO
			}
			
			if (!acceptsRanges)
				nConnections = 1;
			chunkSize = (long) Math.ceil(fsize * 1.0 / nConnections);
			dlinfo = new DLInfo(URL, fsize, chunkSize, acceptsRanges);
//			if (response.containsHeader("")) {
//				Header[] hs = response.getHeaders("");
//				serverChecksum = hs[1].getValue();
//			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				response.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


		// Thread pool
		ExecutorService executor = Executors.newFixedThreadPool(nConnections);
		List<ChunkThread> threads = new ArrayList<ChunkThread>(nConnections);
		List<Future<ChunkThreadResults>> futures = new ArrayList<>();
		

		try {
			// TODO: check server supports this many simultaneous connections
			for (int i = 0; i < nConnections; ++i) {
				ChunkThread ct = new ChunkThread(i, dlinfo, httpclient);
				threads.add(ct);
				futures.add(executor.submit(ct));
//				futures.get(futures.size()-1).get();
				
			}

			System.out.println("waiting...");
			boolean allDone;
			while(true) {
				allDone = true;
				for (Future<ChunkThreadResults> future : futures) { 
					if (! future.isDone() ) {
						allDone = false;
						break;
					}
				}
				if (allDone) break;
				else Thread.sleep(100);
			}
			
//			futures = executor.invokeAll(threads);
//			executor.in
			executor.shutdown(); // stop executor from waiting for more tasks to
									// be added to the pool
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
		} finally {
			httpclient.close();
		}

		if (futures == null) {
			// TODO
		}

		System.out.println("\nDone:");
		long total = 0;
		MessageDigest md = MessageDigest.getInstance("MD5");

		for (Future<ChunkThreadResults> future : futures) {
			System.out.println("+ " + future.get().bytesDownloaded);
			total += future.get().bytesDownloaded;
			md.update(future.get().checksum);
		
		}
		System.out.println("checksum: " + new BigInteger(1, md.digest()).toString(16));
		System.out.println("serverChecksum: " + serverChecksum);
		System.out.println("total downloaded bytes: " + total + "/" + fsize);
		mergeChunks();

	}
	
	private HttpRequestRetryHandler buildRetryHandle() {
		return new HttpRequestRetryHandler() {
			@Override
			public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
				System.err.println(exception.getMessage());
		        if (executionCount >= 5) {
		            // Do not retry if over max retry count
		            return false;
		        }
		        if (exception instanceof InterruptedIOException) {
		            // Timeout
		            return false;
		        }
		        if (exception instanceof UnknownHostException) {
		            // Unknown host
		            return false;
		        }
		        if (exception instanceof ConnectTimeoutException) {
		            // Connection refused
		            return false;
		        }
		        if (exception instanceof SSLException) {
		            // SSL handshake exception
		            return false;
		        }
		        HttpClientContext clientContext = HttpClientContext.adapt(context);		        
		        HttpRequest request = clientContext.getRequest();
		        boolean idempotent = !(request instanceof HttpEntityEnclosingRequest);
		        if (idempotent) {
		            // Retry if the request is considered idempotent
		            return true;
		        }
				return false;
			}
		};
	}

	void foo(DLInfo dlinfo) throws IOException, NoSuchAlgorithmException, InterruptedException, ExecutionException {

	}
	
	/**
	 * Get the size of content of a URL, if known. <br>
	 * long data type will overflow if file size > approx. 9000 Petabytes
	 * 
	 * @param url
	 * @return the file size: -1 if unknown
	 */
	private long getFileSize(CloseableHttpResponse response) {
		long ret = -1;

		Header[] hs = response.getAllHeaders();
		for (int i = 0; i < hs.length; i++)
			System.out.println(hs[i].toString());
		System.out.println("Status: " + response.getStatusLine().getStatusCode() + ". "
				+ response.getStatusLine().getReasonPhrase());
		if (response.getStatusLine().getStatusCode() == 206  
			&& response.containsHeader(HttpHeaders.CONTENT_RANGE)) {
			String contentRange = response.getFirstHeader(HttpHeaders.CONTENT_RANGE).getValue();
			ret = Long.parseLong(contentRange.substring(
					1 + contentRange.lastIndexOf("/"))
				);
			System.out.println("fsize: " + contentRange);
		}
		else if (response.containsHeader(HttpHeaders.CONTENT_LENGTH)) {
			String contentLength = response.getFirstHeader(HttpHeaders.CONTENT_LENGTH).getValue();
			ret = Long.parseLong(contentLength);
			System.out.println("fsize: " + contentLength);
		} else {
			System.out.println("unknown file size");
		}

		return ret;
	}

	/**
	 * Check if the server allows byte-range requests
	 * 
	 * @param response the server's http response object
	 * @return true if ranged GET requests are allowed
	 */
//	private boolean testAcceptRanges(CloseableHttpResponse response) {
//		// if we get Status 206 or "Accept-Ranges: bytes" header
//		if (response.containsHeader(HttpHeaders.ACCEPT_RANGES))
//			return response.getFirstHeader(HttpHeaders.ACCEPT_RANGES).getValue().equals("bytes");
//		return response.getStatusLine().getStatusCode() == 206;
//	}

	private void mergeChunks() throws IOException, NoSuchAlgorithmException {
		if (nConnections > 1) {
			System.out.println("Merging chunks...");
			FileOutputStream p0 = null;
			FileInputStream partFile = null;
			try {
				p0 = new FileOutputStream("p0.part", true);
				FileChannel p0ch = p0.getChannel();
				for (int i = 1; i < nConnections; i++) {
					partFile = new FileInputStream("p" + i + ".part");
					FileChannel partCh = partFile.getChannel();
					p0.getChannel().transferFrom(partCh, p0ch.position(), partCh.size());
					System.out.println(partCh.size());
					p0ch.force(false);
					partFile.close();
					System.out.println("merged p" + i);
				}
				System.out.println("Merging Done!!");

			} catch (Exception e) {
				System.err.println("Merge Error: " + e.getMessage());
				e.printStackTrace();

			} finally {
				if (p0 != null)
					p0.close();
				if (partFile != null)
					partFile.close();
			}
		}

		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] data = new byte[1024 * 512];
		BufferedInputStream bufferIn = new BufferedInputStream(new FileInputStream("p0.part"));
		int bytesRead = -1;
		while ((bytesRead = bufferIn.read(data)) != -1)
			md.update(data, 0, bytesRead);

		System.out.println("checksum of merged: " + new BigInteger(1, md.digest()).toString(16));
		// MD5: 4146168a0ea3e634f6707d9a189cb5e4
	}

	public static void main(String[] args) throws Exception {
//		 new Tachyon("https://storage.googleapis.com/vimeo-test/work-at-vimeo-2.mp4");
		// new Tachyon("https://66skyfiregce-vimeo.akamaized.net/exp=1518823952~acl=%2F130695665%2F%2A~hmac=1765dfb0a222a76006bfcc16b84a74c1db5a8f987db6661c22d1ee16bb0a9544/130695665/sep/audio/379792298/chop/segment-2.m4s");
//		 new Tachyon("https://github.com/sam46/Tachyon/blob/master/app5.avi");
//		 new Tachyon("http://192.168.232.1/app5.avi");
		new Tachyon("http://192.168.232.1/work-at-vimeo-2.mp4");
//		 new Tachyon("http://192.168.232.1/nonexistent.mp4");
	}

}

class DLInfo {
	public final String URL;
	public final long fsize; // file size in bytes. Bytes 0 to fsize-1
	public final long chunkSize; // file is broken up into smaller units of this size
	public final boolean acceptsRanges;

	public DLInfo(String url, long fsize, long chunkSize, boolean acceptsRanges) {
		this.URL = url;
		this.fsize = fsize;
		this.chunkSize = chunkSize;
		this.acceptsRanges = acceptsRanges;
	}
}

class ChunkThread implements Callable<ChunkThreadResults> {
	private final CloseableHttpClient httpClient;
	private final HttpGet httpget;

	private final int id;
	
	private final long startByte;
	private final long endByte;
	private final boolean doCheckRange;
	private final boolean acceptsRanges;
	// private final long fsize;

	private boolean finished = false;
	private long last_saved_byte = -1; // should be between in range [0, this chunk's size)

	private final int BUFSIZE = 1024 * 256;

	public ChunkThread(int id, DLInfo dlinfo, CloseableHttpClient httpClient) {
		this.httpClient = httpClient;
		this.id = id;
		this.acceptsRanges = dlinfo.acceptsRanges;
		httpget = new HttpGet(dlinfo.URL);
		
		if (acceptsRanges) {
			doCheckRange = true;
			startByte = id * dlinfo.chunkSize;
			endByte = Math.min((id + 1) * dlinfo.chunkSize - 1, dlinfo.fsize - 1);
			httpget.addHeader(HttpHeaders.RANGE, "bytes=" + startByte + "-" + endByte);
		} else {
			doCheckRange = false;
			startByte = -1;
			endByte = -1;
		}
		// fsize = dlinfo.fsize;
	}

	@Override
	public ChunkThreadResults call() {
		ChunkThreadResults result = null;
		byte[] checksum = null;
		try {
			// System.out.println(id + " - about to get something from " +
			// httpget.getURI());
			if(acceptsRanges && last_saved_byte > 0) { // for resuming
				httpget.removeHeaders(HttpHeaders.RANGE);
				httpget.addHeader(HttpHeaders.RANGE, "bytes=" + (startByte + last_saved_byte) + "-" + endByte);
			}
			CloseableHttpResponse response = httpClient.execute(httpget);
			int status = response.getStatusLine().getStatusCode();
			if((status != 206 && acceptsRanges) || !(status >= 200 && status < 300)) {
				if (acceptsRanges) {
					// probably reached max server's allowed connections. need to reschedule.
					System.out.println(id+" - Can't recieve partial content. quitting...");
					response.close();
					return result;
				} else {
					// ???
				}
			}

			BufferedInputStream bufferIn = null;
			BufferedOutputStream bufferOut = null;
			try {
//				System.out.println(id + " - established connection. Response: "
//						+ response.getStatusLine().getStatusCode() + ". " + response.getStatusLine().getReasonPhrase());
				
				Header[] hs = response.getAllHeaders();
//				for (int i = 0; i < hs.length; i++)
//					System.out.println(id + " - " + hs[i].toString());
					
				HttpEntity entity = response.getEntity();
				System.out.println(id + " - entity size = " + entity.getContentLength() / 1024 + "KB + "
						+ (entity.getContentLength() - (entity.getContentLength() / 1024) * 1024) + "B");
				if (entity != null) {
					bufferIn = new BufferedInputStream(entity.getContent(), BUFSIZE);
					bufferOut = new BufferedOutputStream(new FileOutputStream("p" + id + ".part", acceptsRanges), BUFSIZE);
					byte[] data = new byte[BUFSIZE];

					MessageDigest md = MessageDigest.getInstance("MD5");
					// DigestInputStream dis = new DigestInputStream(bufferIn,
					// md);

					int bytesRead = -1;
					while ((bytesRead = bufferIn.read(data)) != -1) {
						md.update(data, 0, bytesRead);
						bufferOut.write(data, 0, bytesRead);
						last_saved_byte += bytesRead;

						if (doCheckRange && last_saved_byte > endByte - startByte) {
							// TODO: handle: server sent more data than we
							// expected, could be malicious server?
							throw new Exception("Got extra data at chunk #" + id);
						}
					}
					checksum = md.digest();
					result = new ChunkThreadResults(last_saved_byte + 1, checksum);
				}
			} finally {
				response.close();
				bufferIn.close();
				bufferOut.close();
			}

			System.out.println(id + " - finished execution. saved " + (last_saved_byte + 1) + " Bytes");
			if (doCheckRange && last_saved_byte != endByte - startByte) {
				throw new Exception("downloaded bytes != chunk size");
			}
		} catch (Exception e) {
			System.err.println(id + " - error: " + e.getMessage());
		}

		return result;
	}

}

class ChunkThreadResults {
	public final long bytesDownloaded;
	public final byte[] checksum;

	public ChunkThreadResults(long bytesDownloaded, byte[] checksum) {
		this.bytesDownloaded = bytesDownloaded;
		this.checksum = checksum;
	}
}
