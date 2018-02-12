import java.io.IOException;
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
import org.apache.http.util.EntityUtils;


public class Tachyon {
	static String URL = "https://storage.googleapis.com/vimeo-test/work-at-vimeo-2.mp4";
	static private long fsize; // file size in bytes. Bytes 0 to fsize-1
	static private long chunkSize = 1024*100; // breakup the file into smaller units of this size
	static private int nChunks; // total number of chunks: size/chunkSize
	static private int nConnections = 8; // concurrent connections: number of chunks we'll download in parallel. Will be used for both number of connections and number of concurrent threads
	
	public Tachyon() {
		
		// Thread pool
		ExecutorService executor = Executors.newFixedThreadPool(nConnections);
		
		// Connections pool
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(nConnections);
        cm.setDefaultMaxPerRoute(nConnections);
        
        CloseableHttpClient httpclient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();

		fsize = getFileSize(URL);
		nChunks = (int) Math.ceil(fsize*1.0/chunkSize);
		DLInfo dlinfo = new DLInfo(URL, fsize, chunkSize);
		
		for(int i=0; i < nChunks; ++i) { // if file too big, may run out of memory?
			long startByte = i*1L*chunkSize;
			long endByte = Math.min((i+1L)*chunkSize - 1, fsize-1);
			ChunkThread ct = new ChunkThread(i, dlinfo, httpclient);
			Future future = executor.submit(ct);
		}
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
			; // todo
		}
		
		return ret;
	}
	
	public static void main(String[] args) {
		new Tachyon();
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

class ChunkThread implements Runnable {
    private final CloseableHttpClient httpClient;
    private final HttpGet httpget;

	private final int id;
	private final long startByte;
	private final long endByte;

	private boolean finished = false;
    private long last_downloaded_byte = -1L;
	
    public ChunkThread(int id, DLInfo dlinfo, CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
        this.id = id;
		startByte = id*1L*dlinfo.chunkSize;
		endByte = Math.min((id+1L)*dlinfo.chunkSize - 1, dlinfo.fsize-1);
		httpget = new HttpGet(dlinfo.URL);
		httpget.addHeader(HttpHeaders.RANGE, "bytes="+startByte+"-"+endByte);
	}
    
	@Override
	public void run() {
        try {
            System.out.println(id + " - about to get something from " + httpget.getURI());
            CloseableHttpResponse response = httpClient.execute(httpget);
            
            try {
                System.out.println(id + " - established connection. got response!!");
                // get the response body as an array of bytes
                HttpEntity entity = response.getEntity();

                System.out.println(id + " - entity size (KB) = "+entity.getContentLength()/1024);
                if (entity != null) {
                    byte[] bytes = EntityUtils.toByteArray(entity);
                    System.out.println(id + " - " + bytes.length + " bytes read");
                }
            } finally {
                response.close();
            }
        } catch (Exception e) {
            System.out.println(id + " - error: " + e);
        }

        System.out.println(id + " - finished execution ");
	}
	
}
