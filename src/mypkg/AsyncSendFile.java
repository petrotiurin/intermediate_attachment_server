package mypkg;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;

public class AsyncSendFile implements Callable<String>{
	
	private URL url;
	private String revId;
	private String docId;
	private FileChannel fc;
	
	public AsyncSendFile(URL url, String revId, String docId) throws IOException {
		this.url = url;
		this.revId = revId;
		this.docId = docId;
		this.fc = FileChannel.open(Paths.get("out_"+docId), StandardOpenOption.READ);
	}

	@Override
	public String call() throws Exception {
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("PUT");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("If-Match", revId);
		httpCon.setChunkedStreamingMode(4096);
		OutputStream out = httpCon.getOutputStream();
		bufferredCopy(fc,out);
		out.close();
		InputStream response = httpCon.getInputStream();
		String resp_str = IOUtils.toString(response);
		resp_str = resp_str.trim();
		response.close();
		Files.delete(Paths.get("out_"+docId));
		Files.delete(Paths.get("in_"+docId));
		System.out.println("Deleted.");
		return resp_str;
	}

	private static void bufferredCopy(FileChannel fc, OutputStream out) throws IOException{
		 byte[] buffer = new byte[4096];
		    int length;
		    while ((length = fc.read(ByteBuffer.wrap(buffer))) > 0) {
		    	out.write(buffer, 0, length);
		    }
		    out.flush();
	}
}
