import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.security.MessageDigest;
import java.util.concurrent.Callable;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * 
 * A worker thread that gets parameters via a {@link CThreadHints} object, downloads a chunk, and return the results 
 * via a {@link CThreadHints} object
 * <br>
 * Caller should keep object to reschedule and resume chunk download
 * <br>
 * Will return a {@link CThreadResults} object on each trial
 * 
 */
public class ChunkThread implements Callable<CThreadResults> {
	private final CloseableHttpClient httpClient;
	private final HttpGet httpget;
	private int runCount = 0; // keep track of call times (retries) for each chunk 
	private final int id; // A sequential number to identify this Chunk/thread
	private final long startByte;
	private final long endByte;
	private final boolean doCheckRange;
	private final boolean acceptsRanges;
	private Crc32c crc;
	private long last_saved_byte = -1; // should be between in range [0, this chunk's size)

	private final int BUFSIZE = 1024 * 256;
	private final String path;

	/**
	 *
	 * @param id A sequential number to identify this Chunk/thread
	 * @param ctHints {@link CThreadHints} contains parameters like URL and chunk save location
	 * @param httpClient used to make GET requests
	 */
	public ChunkThread(int id, CThreadHints ctHints, CloseableHttpClient httpClient) {
		this.httpClient = httpClient;
		this.id = id;
		this.path = ctHints.dir+"\\"+ctHints.fname;
		this.acceptsRanges = ctHints.acceptsRanges;
		httpget = new HttpGet(ctHints.URL);
		crc = new Crc32c();
		if (acceptsRanges) {
			doCheckRange = true;
			startByte = id * ctHints.chunkSize;
			endByte = Math.min((id + 1) * ctHints.chunkSize - 1, ctHints.fsize - 1);
			httpget.addHeader(HttpHeaders.RANGE, "bytes=" + startByte + "-" + endByte);
		} else {
			doCheckRange = false;
			startByte = -1;
			endByte = -1;
		}
	}

	@Override
	public CThreadResults call() {		
		runCount++;
		CThreadResults result = CThreadResults.getFailed(id, runCount);

		try {
			if (acceptsRanges && last_saved_byte > 0) { // for resuming
				httpget.removeHeaders(HttpHeaders.RANGE);
				httpget.addHeader(HttpHeaders.RANGE, "bytes=" + (startByte + last_saved_byte) + "-" + endByte);
			}
			
			CloseableHttpResponse response = httpClient.execute(httpget);
			int status = response.getStatusLine().getStatusCode();
			if ((status != 206 && acceptsRanges) || !(status >= 200 && status < 300)) {
				if (acceptsRanges) { // we were expecting a 206
					// probably reached max server's allowed connections. need to reschedule.

					System.err.println(id+" - Can't recieve partial content. quitting...");
					result.msg = CThreadResults.MAX_CON_EXCEEDED;
					
				} else { // some other status code. reschedule anyways
					System.err.println(id+" - Can't recieve content. Status "+response.getStatusLine());
				}
				response.close();
				return result;
			}

			BufferedInputStream bufferIn = null;
			BufferedOutputStream bufferOut = null;
			try {
				System.out.println("connection "+id+" - receiving data...");
				
				HttpEntity entity = response.getEntity();
				System.out.println(id + " - entity size = " + entity.getContentLength() / 1024 + "KB + "
						+ (entity.getContentLength() - (entity.getContentLength() / 1024) * 1024) + "B");
				if (entity != null) {
					bufferIn = new BufferedInputStream(entity.getContent(), BUFSIZE);
					bufferOut = new BufferedOutputStream(new FileOutputStream(path+"_" + id + ".part", acceptsRanges), BUFSIZE);
					byte[] data = new byte[BUFSIZE];


					int bytesRead = -1;
					while ((bytesRead = bufferIn.read(data)) != -1) {
//						crc.update(data, 0, bytesRead);  // TODO uncomment upon implementing combine()
						bufferOut.write(data, 0, bytesRead);
						last_saved_byte += bytesRead;

						if (doCheckRange && last_saved_byte > endByte - startByte) {
							// TODO: handle: server sent more data than we
							// expected, could be malicious server?
							throw new Exception("Got extra data at chunk #" + id);
						}
					}
					result = new CThreadResults(id, runCount, last_saved_byte + 1, crc, true, CThreadResults.SUCCESS);		
				}
			} finally {
				if (response != null) response.close();
				if (bufferIn != null) bufferIn.close();
				if (bufferOut != null) bufferOut.close();
			}

			System.out.println(id + " - finished execution. saved " + (last_saved_byte + 1) + " Bytes");
			if (doCheckRange && last_saved_byte != endByte - startByte) {
				throw new Exception("downloaded bytes != chunk size");
			}
		} catch (Exception e) {
			System.err.println(id + " - Exception: " + e.getMessage());
			e.printStackTrace();
		}

		return result;
	}
}
