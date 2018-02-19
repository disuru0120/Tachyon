public class CThreadHints {
	public final String URL;
	public final long fsize; // file size in bytes. Bytes 0 to fsize-1
	public final long chunkSize; // file is broken up into smaller units of this size
	public final boolean acceptsRanges;

	public CThreadHints(String url, long fsize, long chunkSize, boolean acceptsRanges) {
		this.URL = url;
		this.fsize = fsize;
		this.chunkSize = chunkSize;
		this.acceptsRanges = acceptsRanges;
	}
}