package mypkg;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EnumSet;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.commons.io.IOUtils;

import weka.core.PropertyPath.Path;

import java.nio.file.attribute.BasicFileAttributes;
/**
 * Servlet implementation class MainServlet
 */
@WebServlet("/MainServlet")
public class MainServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	private static String PATH = "potato1";
	private static String SERVER = "127.0.0.1";
	private static String PORT = "5984";
	
	private FileOutputStream fs = null;
	
    public MainServlet() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	      // Allocate a output writer to write the response message into the network socket
	      // TODO: change headers to parameters
	      String docId = request.getHeader("DocId");
	      String revId = request.getHeader("RevId");
	      
	      try {
	    	  File f = new File(docId); 
	    	  if (!f.isFile()) {
	    	      PrintWriter out = response.getWriter();
	    		  String file = getAttachment(docId, revId);
	    		  // output the length
	    		  out.println(new File(file).length());
	    		  System.out.println("Got the file");
	    	  } else {
	    		  // Use output stream to write binary data
	    	      OutputStream os = response.getOutputStream();
	    		  int start = Integer.parseInt(request.getHeader("Start"));
	    		  int end = Integer.parseInt(request.getHeader("End"));
	    		  if (end > f.length()) end = (int) f.length(); // TODO: safe cast.
	    		  byte[] buffer = new byte[end - start];
	    		  FileInputStream in = new FileInputStream(f);
	    		  in.skip(start);
	    		  in.read(buffer);
	    		  response.setContentLengthLong(end - start);
	    		  response.setContentType("application/octet-stream");
	    		  os.write(buffer);
	    		  os.flush();
	    		  os.close();
	    		  in.close();
	    	  }
	      } finally {
	      }
	}
	
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Allocate a output writer to write the response message into the network socket
	      PrintWriter out = response.getWriter();
	      InputStream is = request.getInputStream();
	      
	      String docId = request.getHeader("DocId");
	      String revId = request.getHeader("RevId");

    	  FileChannel fc = FileChannel.open(Paths.get(docId), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    	  
	      try {
	    	  if (revId != null) {
		    	  // Send it to the database.
		    	  fc.close();
		    	  FileInputStream fi = new FileInputStream(docId);
		    	  String resp_json = this.sendAttachment(fi, docId, revId);
		    	  out.println(resp_json);
		    	  Files.delete(Paths.get(docId));
		      } else {
				  int start = Integer.parseInt(request.getHeader("Start"));
				  int end = Integer.parseInt(request.getHeader("End"));
		    	  fc.position(start);
		    	  byte[] buffer = new byte[end-start];
		    	  is.read(buffer);
		    	  fc.write(ByteBuffer.wrap(buffer));
		    	  out.println("Chunk received");
		      }		      
	      } finally {
	    	  is.close();
	    	  out.close();
	    	  fc.close();
	      }
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
		String resp_str = convertStreamToString(response);
		response.close();
		return resp_str;
	}
	
	// Read server response into string
	private static String convertStreamToString(InputStream in) throws IOException{
	    InputStreamReader is = new InputStreamReader(in);
		StringBuilder sb=new StringBuilder();
		BufferedReader br = new BufferedReader(is);
		String read = br.readLine();
		while(read != null) {
		    sb.append(read);
		    read =br.readLine();
		}
		return sb.toString();
	}
}
