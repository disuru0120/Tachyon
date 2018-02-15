import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class Tachyon {
	private String URL;
	static private long fsize; // file size in bytes. Bytes 0 to fsize-1
//	static private long chunkSize = 1024*100; // breakup the file into smaller units of this size
//	static private int nChunks; // total number of chunks: size/chunkSize
	static private int nConnections = 8; // concurrent connections: number of chunks we'll download in parallel. Will be used for both number of connections and number of concurrent threads
	
	public Tachyon(String url) throws Exception{
		this.URL = url;
		// Thread pool
		ExecutorService executor = Executors.newFixedThreadPool(nConnections);
		List<ChunkThread> threads = new ArrayList<ChunkThread>(nConnections);
		List<Future<Long>> futures = null;
		
		// Connections pool
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(nConnections);
        cm.setDefaultMaxPerRoute(nConnections);
        
        CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();

		fsize = getFileSize(URL);
		long chunkSize = (long) Math.ceil(fsize*1.0/nConnections);
		DLInfo dlinfo = new DLInfo(URL, fsize, chunkSize);
		for(int i=0; i < nConnections; ++i) { 
//			long startByte = i*1L*chunkSize;
//			long endByte = Math.min((i+1L)*chunkSize - 1, fsize-1);
			ChunkThread ct = new ChunkThread(i, dlinfo, httpclient);
			threads.add(ct);
		}
		try {
			futures = executor.invokeAll(threads);
			executor.shutdown(); // stop executor from waiting for more tasks to be added to the pool	
		} finally {
			httpclient.close();
		}
		
		if(futures == null) {
			// TODO
		}
		
		System.out.println("\nDone:");
		long total = 0;
		for (Future<Long> future: futures) {
			System.out.println("+ "+future.get());
			total += future.get();
		}
		
		System.out.println("total downloaded bytes: "+total+"/"+fsize);
	}

	private long getFileSize(String url) {
		long ret = -1L;

		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpGet httpGet = new HttpGet(url);
		CloseableHttpResponse response = null;
		try {
			response = httpclient.execute(httpGet);
			Header contentLength = response.getFirstHeader(HttpHeaders.CONTENT_LENGTH);
			
			ret = Long.parseLong(contentLength.getValue());
			System.out.println(contentLength);
			
//       	 Header [] hs = response.getAllHeaders();
//       	 for (int i = 0; i < hs.length; i++) 
//				System.out.println(hs[i].toString());

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
		// The underlying HTTP connection is still held by the response object
		// to allow the response content to be streamed directly from the network socket.
		// In order to ensure correct deallocation of system resources
		// the user MUST call CloseableHttpResponse#close() from a finally clause.
		// Please note that if response content is not fully consumed the underlying
		// connection cannot be safely re-used and will be shut down and discarded
		// by the connection manager. 
		
		if(ret == -1) {
			// TODO
		}
		
		return ret;
	}
	
	public static void main(String[] args) throws Exception {
//		new Tachyon("https://storage.googleapis.com/vimeo-test/work-at-vimeo-2.mp4");
		new Tachyon("https://66skyfiregce-vimeo.akamaized.net/exp=1518680502~acl=%2F130695665%2F%2A~hmac=e8891caf8ba4ad8e0512e3905ae0052b0067cf92e2a128eb0ef411f3c6d18a34/130695665/sep/video/379792293/chop/segment-1.m4s");
	}
}

class DLInfo {
	public final String URL; 
	public final long fsize; // file size in bytes. Bytes 0 to fsize-1
	public final long chunkSize; // breakup the file into smaller units of this fsize
//	public static final int nChunks; // total number of chunks: size/chunkSize
	
	public DLInfo(String url, long fsize, long chunkSize) {
		this.URL = url;
		this.fsize= fsize;
		this.chunkSize = chunkSize;
	}
}

class ChunkThread implements Callable<Long> {
    private final CloseableHttpClient httpClient;
    private final HttpGet httpget;

	private final int id;
	private final long startByte;
	private final long endByte;
//	private final long fsize;

	private boolean finished = false;
    private long last_saved_byte = -1L; // should be between in range [0, this chunk's size)
    
    private final int BUFSIZE = 1024*8;
	
    public ChunkThread(int id, DLInfo dlinfo, CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
        this.id = id;
		startByte = id*dlinfo.chunkSize;
		endByte = Math.min((id+1)*dlinfo.chunkSize - 1, dlinfo.fsize-1);
		httpget = new HttpGet(dlinfo.URL);
		httpget.addHeader(HttpHeaders.RANGE, "bytes="+startByte+"-"+endByte);
//		fsize = dlinfo.fsize;
	}
    
	@Override
	public Long call() {
//		long checksum = 0;
        try {
//            System.out.println(id + " - about to get something from " + httpget.getURI());
            CloseableHttpResponse response = httpClient.execute(httpget);
            BufferedInputStream bufferIn = null;
            BufferedOutputStream bufferOut = null;
            try {
//            	 Header [] hs = response.getAllHeaders();
//            	 for (int i = 0; i < hs.length; i++) {
//					System.out.println(id+" - "+hs[i].toString());
//				}
            	System.out.println(id + " - established connection. got response!!");
                HttpEntity entity = response.getEntity();
                System.out.println(id + " - entity size = " + entity.getContentLength()/1024 + "KB + " +
                		(entity.getContentLength()-(entity.getContentLength()/1024)*1024) + "B");
                if (entity != null) {
                	bufferIn = new BufferedInputStream(entity.getContent(), BUFSIZE);
                	bufferOut = new BufferedOutputStream(new FileOutputStream("p"+id+".part",false), BUFSIZE);
                	
                	byte[] data = new byte[BUFSIZE];
                	int bytesRead = -1;
                	while( (bytesRead = bufferIn.read(data)) != -1) {
                		// check bufferIn has enough data? TODO
                		bufferOut.write(data, 0, bytesRead);
                		last_saved_byte += bytesRead;
                		
                		if(last_saved_byte > endByte - startByte) {
                			// TODO: double check the condition for off-by-1 error 
                			// TODO: handle: server sent more data than we expected, could be malicious server
                			throw new Exception("Got extra data at chunk #"+id);
                		}
                		                		
                	}                	
                }
            } finally {
                response.close();
                bufferIn.close();
                bufferOut.close();
            }
            
            System.out.println(id + " - finished execution. saved "+(last_saved_byte+1)+" Bytes");
            if (last_saved_byte != endByte - startByte) {
            	throw new Exception("downloaded bytes != chunk size");
            }
        } catch (Exception e) {
            System.out.println(id + " - error: " + e.getMessage());
        }

        return last_saved_byte + 1;
	}
	
}
