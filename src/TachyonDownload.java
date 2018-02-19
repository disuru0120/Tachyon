import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import javax.net.ssl.SSLException;

import org.apache.commons.codec.binary.Base64;
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

/**
 * 
 * Download a single file given URL and save location
 *
 */
public class TachyonDownload {
	private String URL;
	private long fsize; // file size in bytes. Bytes 0 to fsize-1
	private int nConnections; // concurrent connections: number of chunks
									// we'll download in parallel. Will be used
									// for both number of connections and number
									// of concurrent threads. May get decreased later due to server-imposed limits
	private int nChunks = -1; // how many actual .part files we end up with
	private long chunkSize;
	private String serverChecksum = "";
	private final CloseableHttpClient httpclient;
	private final String path;
	
	private CloseableHttpClient buildHttpClient() {
		// Connections pool
		PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
		cm.setMaxTotal(nConnections);
		cm.setDefaultMaxPerRoute(nConnections);
		
		return HttpClients.custom().setConnectionManager(cm)
				.setRetryHandler(buildRetryHandle())  // will retry a few times on connection errors
				.build();
	}
	
	/**
	 * Do a preliminary GET request to determine the download setting, like whether the server supports multipart downloads
	 * @return {@link CThreadHints} parameters that'll be passed to chunks worker threads {@link ChunkThread}
	 * @throws Exception
	 */
	private CThreadHints getCThreadhints() throws Exception {
		CThreadHints ctHints = null;
		boolean acceptsRanges = false;
		HttpGet httpGet = new HttpGet(this.URL);
		httpGet.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate, sdch");
		httpGet.addHeader(HttpHeaders.RANGE,"bytes=0-1"); // first time try a range request
		CloseableHttpResponse response = null;
		try {	
			System.out.println("GET request:");
			System.out.println(httpGet.getRequestLine());
			Header [] req = httpGet.getAllHeaders();
			for (int i = 0; i < req.length; i++) 
				System.out.println(req[i]);
			System.out.println("------------------\n");
			
			response = httpclient.execute(httpGet);
			int status = response.getStatusLine().getStatusCode();
			if (status == 206) { // range request worked :)
				fsize = getFileSize(response);
				acceptsRanges = true;//fsize != -1 && testAcceptRanges(response);
			}
			else if (status >= 200 && status < 300) { // if range request failed, try again without range
				httpGet.removeHeaders(HttpHeaders.RANGE);
				response.close();
				response = httpclient.execute(httpGet);
				fsize = getFileSize(response);
			} else { 
				throw new Exception("TODO. Got "+response.getStatusLine());
			}
			
			if (!acceptsRanges) {
				nConnections = 1;
				System.out.println("multipart download unsupported!! Will use 1 connection");
			}
			
			Header [] checksumHeaders = response.getHeaders("x-goog-hash");
			for(int i=0; i < checksumHeaders.length; i++) { // TODO add support for more checksum methods
				String value = checksumHeaders[i].getValue();
				if (value.contains("crc32c=")) {
					serverChecksum = value.substring(value.lastIndexOf("crc32c=")+7);
					break;
				}
			}
			
			chunkSize = (long) Math.ceil(fsize * 1.0 / nConnections);
			ctHints = new CThreadHints(URL, fsize, chunkSize, acceptsRanges, path);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(response!=null ) 
					response.close();		
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return ctHints;
	}
	

	/**
	 * 
	 * @param ctHints A CThreadHints object which contains parameters to pass to chunk worker threads
	 * @return true if all chunks downloaded successfully
	 * @throws Exception
	 */
	private boolean downloadChuncks(CThreadHints ctHints) throws Exception {
		System.out.println();
		if (nConnections > 1) System.out.println("Trying with "+nConnections+" connections:");
		CThreadResults [] successfulResults = new CThreadResults[nConnections];
		
		/*** Thread pooling ***/
		final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(nConnections);
		ExecutorService executor = new ThreadPoolExecutor(nConnections, nConnections,
		        0L, TimeUnit.MILLISECONDS, queue);
		CompletionService compService = new ExecutorCompletionService(executor);
		List<ChunkThread> threads = new ArrayList<ChunkThread>(nConnections);
		Set<Future<CThreadResults>> futures = new HashSet<>(); 	// Futures for all submitted Callables that have not yet been checked

		Set<Integer> rejected = new HashSet<>(); // Keep track of rejected connections
		
		nChunks = nConnections; //  we'll have as many chunks (.part files) as we have connections, even tho nConnections may have to get decreased later due to server-imposed limits
		for (int i = 0; i < nConnections; ++i) {
			// TODO: use singleton pattern for ctHints instead
			ChunkThread ct = new ChunkThread(i, ctHints, httpclient);
			threads.add(ct);
			futures.add(compService.submit(ct));
		}
		 
		Future<CThreadResults> completedFuture;
		CThreadResults newResult = null;
		
		while (futures.size() > 0) {
		    // block until a ChunkThread finishes
			completedFuture = compService.take();
		    futures.remove(completedFuture);

		    // Get the results from the ChunkThread:
		    try {
		    	newResult = completedFuture.get(); // this shouldn't block, because it take() already blocked 
		    } catch (ExecutionException e) {
		        System.err.println("ChunkThread #" + (newResult!=null?newResult.id:"?") + " failed. " + e.getCause());
		    } finally {
			    if (newResult != null) {
			    	switch (newResult.msg) {
					case CThreadResults.SUCCESS:
						// chunk downloaded. Store results:
						successfulResults[newResult.id] = newResult;
						break;
					case CThreadResults.MAX_CON_EXCEEDED:
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
					case CThreadResults.UNKNOWN:
						if(newResult.runCount > Settings.CHUNK_ERROR_RETRY) { // give up
							System.err.println(newResult.id+" failed too many times. Giving up!!");
						} else {
							System.err.println(newResult.id+" failed. will run again later");
							futures.add(compService.submit(threads.get(newResult.id)));
						}
						break;
					default:
						System.err.println(newResult.id+"'s newResult.msg="+newResult.msg+" How did we get here?");
						break;
					}
			    } else {
			    	System.err.println("newResult null");   	
			    } 
		    }
		}
		
		System.out.println("\nEnded up using "+ nConnections +" connections");
		executor.shutdown();
		
		int downloadedChunkes = 0; 
		for(int i=0; i < successfulResults.length; i++) {
			if (successfulResults[i] != null) downloadedChunkes++;
		}
		System.out.println("Downloaded "+downloadedChunkes+"/"+nChunks+" chuncks");
		if (downloadedChunkes != nChunks) {
			System.out.println("Some parts failed to download. Please try again later.\n");
			return false;
		}
		System.out.println();
		
		// TODO: compute Crc32c using already computed chunks checksums rather than after merging
		// by implementing a combine() method for crc32c similar to 
		// this https://www.zlib.net/manual.html#Checksum
		/*
		long firstcrc = successfulResults[0].checksum.getValue();
		for (int i=1; i < successfulResults.length; i++) {
			long secondLen = successfulResults[i].bytesDownloaded;
			long secondCrc = successfulResults[i].checksum.getValue();
			firstcrc = Crc32c.combine(firstcrc, secondCrc, secondLen);
		}
		// check serverCrc == firstcrc
		*/
		return true;
	}
	
	/**
	 * Download a single file given URL and save location
	 * @param url address of the file to be downloaded
	 * @param saveAs Full path to output file
	 * @param maxConnections If server supports parallel connections, use this many connections at most. may end up using fewer due to server restrictions
	 * @throws Exception
	 */
	public TachyonDownload(String url, String saveAs, int maxConnections) throws Exception {
		this.URL = url;
		this.path = saveAs;
		this.nConnections = Math.max(maxConnections, 1);
		httpclient = buildHttpClient();
//		boolean success = false;
		Thread mergeThread = null;
		try {
			CThreadHints dlinfo = getCThreadhints();
			mergeThread = new Thread(() -> {
				try {
					String savedPath = TachyonDownload.mergeChunks(nChunks, saveAs+"_0.part");
					System.out.println("\n"+this.URL+" download successful.\n");
					validateFile(savedPath, serverChecksum);
				} catch (Exception e) {
					System.out.println("merging failed");
					e.printStackTrace();
				}
			});
			if (downloadChuncks(dlinfo))
				mergeThread.start();
		} catch (Exception e) {
			System.err.println("downloaded failed");
//			e.printStackTrace();
		} finally {
			httpclient.close();
		}
		if(mergeThread != null)
			mergeThread.join();
	}
	
	/**
	 * Dictates what to do if httplclient.execute(request) failed
	 * @return HttpRequestRetryHandler used to configure an HttpClient object
	 */
	private HttpRequestRetryHandler buildRetryHandle() {
		return new HttpRequestRetryHandler() {
			@Override
			public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
				System.out.println("retrying to connect... ("+executionCount+")");
				System.err.println(exception.getMessage());
		        if (executionCount >= Settings.CONNECTION_ERROR_RETRY) {
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
//		        if (exception instanceof SSLException) {
//		            // SSL handshake exception
//		            return false;
//		        }

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
	 * merge downloaded chunks (.part files) <br>
	 * Expecting parts filenames to have the format x_0.part, x_2.part2 ... x_n-1.part <br>
	 * .part files will be deleted upon success
	 * @param num number of chunks to merge
	 * @param path path to 0th .part file. All n chunks must be present in the directory
	 * @return path to output file or "" if failed
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	private static String mergeChunks(int num, String path) throws IOException {
		String outname = "";
		System.out.println();
		if (num > 1) {
			System.out.println("Merging chunks...");
			FileInputStream partFile = null;
			File p0file = new File(path);
	
			if (!(p0file.exists() && p0file.isFile()) && path.endsWith("_0.part")) {
				System.err.println("invalid path");
				return outname;
			}
			
			String rawName = p0file.getName().replace("_0.part", "");
			final Pattern pattern = Pattern.compile(rawName+"_\\d+?.part");
			String[] files = new File(p0file.getParent()).list(new FilenameFilter() {			
				@Override
				public boolean accept(File dir, String name) {
					int beg = -1, end = -1; 
					Matcher matcher = pattern.matcher(name);
					while (matcher.find()) {
						beg = matcher.start();
						end = matcher.end();
						break;
					}					
					return beg==0 && end==name.length() && new File(dir.getAbsolutePath()+"\\"+name).isFile();
				}
			});
			
			if(files.length != num) {
				System.err.println("missing .part files");
				return outname;
			}

			FileOutputStream p0 = null; // 0th part
			try {
				// append all parts to end of 0th part
				p0 = new FileOutputStream(path, true);
				FileChannel p0ch = p0.getChannel();
				for (int i = 1; i < num; i++) {
					partFile = new FileInputStream(p0file.getParent()+"\\"+rawName + "_" + i + ".part");
					FileChannel partCh = partFile.getChannel();
					p0.getChannel().transferFrom(partCh, p0ch.position(), partCh.size());
					p0ch.force(false);
					System.out.println("merged part " + i + "  "+partCh.size()+" bytes");
					partFile.close();
				}

				System.out.println("Merging Done!!");
				System.out.println("deleting .part files");
				for (int i = 1; i < num; i++) 
					new File(p0file.getParent()+"\\"+rawName + "_" + i + ".part").delete();				

			} catch (Exception e) {
				System.err.println("Merge Error: " + e.getMessage());
				e.printStackTrace();
			} finally {
				if (p0 != null)
					p0.close();
				if (partFile != null)
					partFile.close();
				try {
					File target = new File(p0file.getParent()+"\\"+rawName);
					if (target.exists() && target.isFile()) {
						System.err.println("found file with same name, will overwrite");
						target.delete();
					}
					outname = Files.move(p0file.toPath(), p0file.toPath().resolveSibling(rawName)).toFile().getAbsolutePath();			
				}
				catch (Exception e) {
					e.printStackTrace();
					System.err.println(e.getMessage());
				}
			}
		}
		
		System.out.println("Saved as: " + outname);
		return outname;
	}
	
	/**
	 * Computes file's checksum and compares against a given checksum (if any)
	 * @param serverChecksum  authoritative checksum
	 * @throws IOException
	 */
	private static String validateFile(String path, String serverChecksum) throws IOException {
		System.out.println("Checking file integrity...");
		String ourChecksum = ""; 
		BufferedInputStream bufferIn = null;
		try {
//			MessageDigest md = MessageDigest.getInstance("MD5");
			Crc32c crc  = new Crc32c();
			byte[] data = new byte[1024 * 512];
			bufferIn = new BufferedInputStream(new FileInputStream(path));
			int bytesRead = -1;
			while ((bytesRead = bufferIn.read(data)) != -1) {
//				md.update(data, 0, bytesRead);
				crc.update(data,0,bytesRead);
			}

//			System.out.println("MD5 checksum of merged: " + new BigInteger(1, md.digest()).toString(16));
			Base64 b64 = new Base64();
			ourChecksum = new String(b64.encode(Crc32c.getValueAsBytes(crc.getValue())));
			System.out.println("Crc32c checksum of merged: "+ ourChecksum);
			if(serverChecksum != null && !serverChecksum.isEmpty()) {
				if (serverChecksum.equals(ourChecksum)) {
					System.err.println("Warning. File courrupt. Re-download Advised");
					System.err.println("Checksum mismatch");
				} 
			} else {
				System.err.println("Server's checksum not provided");
			}
			
		} finally {
			bufferIn.close();
		}
		return ourChecksum;
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
}
