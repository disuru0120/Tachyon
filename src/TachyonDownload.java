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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;
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

public class TachyonDownload {
	private String URL;
	private long fsize; // file size in bytes. Bytes 0 to fsize-1
	// static private long chunkSize = 1024*100; // breakup the file into
	// smaller units of this size
	// static private int nChunks; // total number of chunks: size/chunkSize
	private int nConnections = 8; // concurrent connections: number of chunks
									// we'll download in parallel. Will be used
									// for both number of connections and number
									// of concurrent threads. May get decreased later due to server-imposed limits
	private int nChunks = -1; // how many actual .part files we end up with
	private long chunkSize;
	private String serverChecksum = "";
	private final CloseableHttpClient httpclient;
	
	private CloseableHttpClient buildHttpClient(){
		// Connections pool
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(nConnections);
		cm.setDefaultMaxPerRoute(nConnections);
		
		return HttpClients.custom().setConnectionManager(cm)
//				.setRetryHandler(buildRetryHandle())
				.build();
	}
	
	private DLInfo getDLInfo() throws Exception{

		boolean acceptsRanges = false;
		HttpGet httpGet = new HttpGet(this.URL);
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
		return dlinfo;
	}
	
	private void foo(DLInfo dlinfo) throws Exception {

		/*** Thread pooling ***/
		final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(nConnections);
		ExecutorService executor = new ThreadPoolExecutor(nConnections, nConnections,
		        0L, TimeUnit.MILLISECONDS,
		        queue);
		
//		ExecutorService executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler)
		CompletionService compService = new ExecutorCompletionService(executor);
		List<ChunkThread> threads = new ArrayList<ChunkThread>(nConnections);
		Set<Future<ChunkThreadResults>> futures = new HashSet<>(); 	// Futures for all submitted Callables that have not yet been checked

		Set<Integer> rejected = new HashSet<>(); // Keep track of rejected connections
		System.out.println("waiting..."); 
		
		nChunks = nConnections; //  we'll have as many chunks (.part files) as we have connections, even tho nConnections may have to get decreased later due to server-imposed limits
		// TODO: check server supports this many simultaneous connections
		for (int i = 0; i < nConnections; ++i) {
			ChunkThread ct = new ChunkThread(i, dlinfo, httpclient);
			threads.add(ct);
			futures.add(compService.submit(ct));
		}
		 
		Future<ChunkThreadResults> completedFuture;
		ChunkThreadResults newResult = null;
		
		System.out.println("futures.size = "+futures.size());
		while (futures.size() > 0) {
		    // block until a ChunkThread finishes
//		    completedFuture = compService.poll(100, TimeUnit.MILLISECONDS);
//		    if(completedFuture == null) continue;
			completedFuture = compService.take();
		    futures.remove(completedFuture);

		    // Get the result from the ChunkThread:
		    try {
		    	newResult = completedFuture.get(); // this shouldn't block, because it already blocked using take() 
		    } catch (ExecutionException e) {
		        Throwable cause = e.getCause();
		        System.err.println("ChunkThread #"+(newResult!=null?newResult.id:"?")+" failed. "+cause);
//		        for (Future<ChunkThreadResults> f: futures) {
//		            // pass true if you wish to cancel in-progress Callables as well as
//		            // pending Callables
//		            f.cancel(true);
//		        }	 
//		        break;
		    } finally {
			    if (newResult != null) {
			    	switch (newResult.msg) {
					case ChunkThreadResults.SUCCESS:
						
						break;
					case ChunkThreadResults.MAX_CON_EXCEEDED:
						// reschedule the connection/thread to run later and decrease the number of connections. but don't allow the same rejected connection to decrease nConnections more than once
						if (!rejected.contains(newResult.id)) { 
							nConnections--;
							((ThreadPoolExecutor)executor).setCorePoolSize(nConnections);
							((ThreadPoolExecutor)executor).setMaximumPoolSize(nConnections);
							rejected.add(newResult.id);
						}
						System.err.println("Max Connections exceeded. "+newResult.id+" will run again later");
						futures.add(compService.submit(threads.get(newResult.id)));
						break;
					default:
						break;
					}
			    } else {
			    	System.err.println("newResult null");
			    	
			    } 
		    }
		}
		
		System.out.println("Server allows "+ nConnections +" connections");
		executor.shutdown();
	///////////////////////////////////////////////////////////
		

//		try {
//			// TODO: check server supports this many simultaneous connections
//			for (int i = 0; i < nConnections; ++i) {
//				ChunkThread ct = new ChunkThread(i, dlinfo, httpclient);
//				threads.add(ct);
//				futures.add(executor.submit(ct));
////				futures.get(futures.size()-1).get();
//				
//			}
//
//			boolean allDone;
//			while(true) {
//				allDone = true;
//				for (Future<ChunkThreadResults> future : futures) { 
//					if (! future.isDone() ) {
//						allDone = false;
//						break;
//					}
//				}
//				if (allDone) break;
//				else Thread.sleep(100);
//			}
//			
////			futures = executor.invokeAll(threads);
////			executor.in
//			executor.shutdown(); // stop executor from waiting for more tasks to
//									// be added to the pool
//			
//		} catch (Exception e) {
//			System.err.println(e.getMessage());
//		} finally {
//			httpclient.close();
//		}

//		if (futures == null) {
//			// TODO
//		}

		System.out.println("\nDone:");
		long total = 0;
		MessageDigest md = MessageDigest.getInstance("MD5");
		System.out.println("futures.size = "+futures.size());
		for (Future<ChunkThreadResults> future : futures) {
			System.out.println("+ " + future.get().bytesDownloaded);
			total += future.get().bytesDownloaded;
			md.update(future.get().checksum);
		}
		System.out.println("checksum: " + new BigInteger(1, md.digest()).toString(16));
		System.out.println("serverChecksum: " + serverChecksum);
		System.out.println("total downloaded bytes: " + total + "/" + fsize);
	}
	
	public TachyonDownload(String url) throws Exception {
		this.URL = url;
		httpclient = buildHttpClient();
		try {
		DLInfo dlinfo = getDLInfo();
		foo(dlinfo);
		} finally {
			httpclient.close();
		}
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
			System.out.println("fsize: " + ret);
		}
		else if (response.containsHeader(HttpHeaders.CONTENT_LENGTH)) {
			String contentLength = response.getFirstHeader(HttpHeaders.CONTENT_LENGTH).getValue();
			ret = Long.parseLong(contentLength);
			System.out.println("fsize: " + ret);
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
		if (nChunks > 1) {
			System.out.println("Merging chunks...");
			FileOutputStream p0 = null;
			FileInputStream partFile = null;
			try {
				p0 = new FileOutputStream("p0.part", true);
				FileChannel p0ch = p0.getChannel();
				for (int i = 1; i < nChunks; i++) {
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
		
		BufferedInputStream bufferIn = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] data = new byte[1024 * 512];
			 bufferIn = new BufferedInputStream(new FileInputStream("p0.part"));
			int bytesRead = -1;
			while ((bytesRead = bufferIn.read(data)) != -1)
				md.update(data, 0, bytesRead);
	
			System.out.println("checksum of merged: " + new BigInteger(1, md.digest()).toString(16));
			// MD5: 4146168a0ea3e634f6707d9a189cb5e4
		} finally {
			bufferIn.close();
		}
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
		ChunkThreadResults result = ChunkThreadResults.getFailed(id);
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
					response.close();
					System.out.println(id+" - Can't recieve partial content. quitting...");
					result.msg = ChunkThreadResults.MAX_CON_EXCEEDED;
//					Thread.sleep(10000);
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
				System.out.println(id+" - "+response.getFirstHeader(HttpHeaders.CONTENT_RANGE));
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
					result = new ChunkThreadResults(id, last_saved_byte + 1, checksum, true, ChunkThreadResults.SUCCESS);
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

