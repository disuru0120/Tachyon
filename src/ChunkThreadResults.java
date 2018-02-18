
public class ChunkThreadResults {
	public int id;
	public long bytesDownloaded;
	public byte[] checksum;
	public boolean finished;
	public int msg = UNKNOWN;

	public ChunkThreadResults(int id, long bytesDownloaded, byte[] checksum, boolean finished, int msg) {
		this.bytesDownloaded = bytesDownloaded;
		this.checksum = checksum;
		this.id = id;
		this.finished = finished;
		this.msg = msg;
	}
	
	public static ChunkThreadResults getFailed(int id) {
		return new ChunkThreadResults(id, -1, null, false, UNKNOWN);
	}

	public static final int MAX_CON_EXCEEDED = 2;
	public static final int SUCCESS = 1;
	public static final int UNKNOWN = 0;
	
}