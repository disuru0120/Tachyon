import java.util.zip.Checksum;

public class CThreadResults {
	public int id;
	public long bytesDownloaded;
	public Checksum checksum;
	public boolean finished;
	public int msg = UNKNOWN;
	public int runCount = 0;

	public CThreadResults(int id, int runCount, long bytesDownloaded, Checksum checksum, boolean finished, int msg) {
		this.bytesDownloaded = bytesDownloaded;
		this.checksum = checksum;
		this.id = id;
		this.finished = finished;
		this.msg = msg;
		this.runCount = runCount;
	}
	
	public static CThreadResults getFailed(int id, int runCount) {
		return new CThreadResults(id, runCount, -1, null, false, UNKNOWN);
	}

	public static final int MAX_CON_EXCEEDED = 2;
	public static final int SUCCESS = 1;
	public static final int UNKNOWN = 0;
//	public static final int 
	
}