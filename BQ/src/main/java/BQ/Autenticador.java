package BQ;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;


/**
 * Servlet implementation class Autenticador
 */
public class Autenticador extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Autenticador() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		GoogleCredentials credentials;
		InputStream in = BQ.class.getResourceAsStream("/Movistar Produccion 3-6563cfd76e7c.json");
		credentials = ServiceAccountCredentials.fromStream(in);
		  

		  // Instantiate a client.
		  BigQuery bigquery =
		      BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
		  

		  // Use the client.
		  
		  System.out.println("Datasets:");
		  for (Dataset dataset : bigquery.listDatasets().iterateAll()) {
		    System.out.printf("%s%n", dataset.getDatasetId().getDataset());
		  }
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
