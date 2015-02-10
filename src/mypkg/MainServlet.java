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
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
	
	private FileOutputStream fs = null;
	
    public MainServlet() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
	      // Allocate a output writer to write the response message into the network socket
	      PrintWriter out = response.getWriter();
	      try {
	         out.println("Hello World!");
	      } finally {
	         out.close(); 
	      }
	}
	
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Allocate a output writer to write the response message into the network socket
	      PrintWriter out = response.getWriter();
	      InputStream is = request.getInputStream();
	      
	      String docId = request.getHeader("DocId");
	      String revId = request.getHeader("RevId");
	      
	      try {
	    	  if (fs == null) {
	    		  fs = new FileOutputStream("out_file.png");
	    	  }
	    	  IOUtils.copy(is,fs);
		      if (docId != null && revId != null) {
		    	  // Send it to the database.
		    	  FileInputStream fi = new FileInputStream("out_file.png");
		    	  String resp_json = this.sendAttachment(fi, docId, revId);
		    	  out.println(resp_json);
		    	  fs.close();
		    	  fs = null;
		      } else {
			      out.println("Chunk received");
		      }
		      
	      } finally {
	    	  is.close();
	    	  out.close();
	      }
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
