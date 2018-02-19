import java.io.File;

/**
 * 
 * Contains parameters that'll be passed to {@link ChunkThread} objects
 * TODO: use singleton pattern for this
 *
 */
public class CThreadHints {
	public final String URL;
	public final long fsize; // file size in bytes. Bytes 0 to fsize-1
	public final long chunkSize; // file is broken up into smaller units of this size
	public final boolean acceptsRanges;
	public final String dir;
	public final String fname;

	public CThreadHints(String url, long fsize, long chunkSize, boolean acceptsRanges, String path) {
		this.URL = url;
		this.fsize = fsize;
		this.chunkSize = chunkSize;
		this.acceptsRanges = acceptsRanges;
		this.dir = new File(path).getParentFile().getAbsolutePath();
		this.fname = new File(path).getName();
	}
}