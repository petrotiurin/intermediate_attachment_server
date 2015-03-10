package mypkg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.commons.io.IOUtils;
/**
 * Servlet implementation class MainServlet
 */
@WebServlet("/MainServlet")
public class MainServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	private static String PATH = "potato1";
	private static String SERVER = "127.0.0.1";
	private static String PORT = "5984";
	
    public MainServlet() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	      // Allocate a output writer to write the response message into the network socket
	      String docId = request.getHeader("DocId");
	      String revId = request.getHeader("RevId");
	      
	      try {
	    	  File f = new File(docId); 
	    	  if (!f.isFile()) {
	    		  // Get the file and output its length
	    		  PrintWriter out = response.getWriter();
	    		  String file = getAttachment(docId, revId);
	    		  out.println(new File(file).length());
	    	  } else if (request.getHeader("Start") == null) {
	    		  // If file present and byte range not specified
	    		  PrintWriter out = response.getWriter();
	    		  out.println(f.length());
	    	  } else {
	    		  // Use output stream to write binary data
	    		  OutputStream os = response.getOutputStream();
	    		  long start = Long.parseLong(request.getHeader("Start"));
	    		  long end = Long.parseLong(request.getHeader("End"));
	    		  if (end > f.length()) end = f.length();
	    		  int chunk_size = (int) (end - start);
	    		  
	    		  // Read bytes from the file
	    		  byte[] buffer = new byte[chunk_size];
	    		  FileInputStream in = new FileInputStream(f);
	    		  in.skip(start);
	    		  in.read(buffer, 0, chunk_size);
	    		  
	    		  // Append checksum at the end
	    		  MessageDigest md = MessageDigest.getInstance("MD5");
	    		  byte[] chunk_with_checksum = new byte[32 + chunk_size];
	    		  byte[] md5 = md.digest(buffer);
	    		  System.arraycopy(buffer, 0, chunk_with_checksum, 0, chunk_size);
	    		  System.arraycopy(bytesToHex(md5).getBytes(), 0, chunk_with_checksum, chunk_size, 32);
	    		  
	    		  // Send chunk
	    		  response.setContentLengthLong(32 + end - start);
	    		  response.setContentType("application/octet-stream");
	    		  os.write(chunk_with_checksum);
	    		  os.flush();
	    		  os.close();
	    		  in.close();
	    	  }
	      } catch (NoSuchAlgorithmException e) {
	    	  e.printStackTrace();
	      }
	}
	
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	      PrintWriter out = response.getWriter();
	      
	      String docId = request.getHeader("DocId");
	      String revId = request.getHeader("RevId");
    	  
	      try {
	    	  if (revId != null) {
	    		  // Send it to the database.
	    		  if (!new File("in_"+docId).exists()) {
	    			  String resp_json = getDocInfo(docId);
	    			  out.println(resp_json);
	    		  } else {
	    			  FileInputStream fi = new FileInputStream("in_"+docId);
	    			  String resp_json = this.sendAttachment(fi, docId, revId);
	    			  out.println(resp_json);
	    			  Files.delete(Paths.get("in_"+docId));
	    		  }
		      } else if (request.getHeader("Start") == null) {
		    	  // Create file
		    	  File f = new File("in_"+docId);
		    	  if (!f.exists()) f.createNewFile();
		    	  response.setStatus(200);
		      } else {
		    	  // Receive file chunk
				  long start = Long.parseLong(request.getHeader("Start"));
				  long end = Long.parseLong(request.getHeader("End"));
	    		  int chunk_size = (int) (end - start);
				  String sent_md5 = request.getHeader("MD5");
		    	  byte[] buffer = new byte[chunk_size];
				  // StandardOpenOption.CREATE,
		    	  FileChannel fc = FileChannel.open(Paths.get("in_"+docId), StandardOpenOption.WRITE);
			      InputStream is = request.getInputStream();
		    	  is.read(buffer);
		    	  fc.position(start);
		    	  MessageDigest md = MessageDigest.getInstance("MD5");
		    	  byte[] buffer_md5 = md.digest(buffer);
		    	  
		    	  // Compare the checksums
		    	  if (sent_md5.equals(bytesToHex(buffer_md5))) {
		    		  int written_bytes = fc.write(ByteBuffer.wrap(buffer));
		    		  if (written_bytes == chunk_size) {
		    			  out.println("Chunk received " + start);
		    		  } else {
			    		  out.println("Not received.");
		    		  }
		    	  } else {
		    		  out.println("Not received.");
		    	  }
		    	  fc.close();
		    	  is.close();
		      }		      
	      } catch (IOException e) {
	    	  out.println("Not received.");
	      } catch (NoSuchAlgorithmException e) {
	    	  e.printStackTrace();
	    	  out.println("Not received.");
	      } finally {
	    	  out.close();
	      }
	}
	
	private String getDocInfo(String docId) throws IOException{
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH + "/" + docId);
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		InputStream response = httpCon.getInputStream();
		String resp_str = IOUtils.toString(response);
		resp_str = resp_str.trim();
		response.close();
		return resp_str;
	}
	
	private String getAttachment(String docId, String revId) throws IOException{
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH + "/" + docId + "/" + "attachment");
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("GET");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("If-Match", revId);
		InputStream response = httpCon.getInputStream();
		String filepath = docId;
		FileOutputStream fs = new FileOutputStream(filepath);
		IOUtils.copy(response,fs);
		response.close();
		fs.close();
		return filepath;
	}
	
	private String sendAttachment(FileInputStream fi, String docId, String revId) throws IOException {
		URL url = new URL("http://" + SERVER + ":" + PORT + "/" + PATH + "/" + docId + "/" + "attachment");
		HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
		httpCon.setDoOutput(true);
		httpCon.setRequestMethod("PUT");
		httpCon.setRequestProperty("Content-Type", "application/octet-stream");
		httpCon.setRequestProperty("If-Match", revId);
		OutputStream out = httpCon.getOutputStream();
		IOUtils.copy(fi,out);
		out.close();
		InputStream response = httpCon.getInputStream();
		String resp_str = IOUtils.toString(response);
		resp_str = resp_str.trim();
		response.close();
		return resp_str;
	}
	
	public static String bytesToHex(byte[] bytes) {
		char[] hexArray = "0123456789ABCDEF".toCharArray();
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
}
