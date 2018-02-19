
public final class Settings {
	private Settings(){}

	public final static int DEFUALT_MAX_CONNECTIONS = 4;  // default max number of parallel connection
	public final static int CONNECTION_ERROR_RETRY = 5; // number of times Httpclient retries to establish connection to server for a given url
	public final static int CHUNK_ERROR_RETRY = 5; // number of times {@link ChunkThread} will be rescheduled on errors
}
