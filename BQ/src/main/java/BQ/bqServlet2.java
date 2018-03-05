package BQ;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;


/**
 * Servlet implementation class bqServlet
 */
public class bqServlet2 extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public bqServlet2() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	@Override
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		
		String commands[] = {"ls", "/"};
		ProcessBuilder pb = new ProcessBuilder(commands);
		pb.start();
		
		Map<String, String> envi = pb.environment();
		System.out.println(envi.toString());
		String Key = "GOOGLE_APPLICATION_CREDENTIALS";
		String Value = "/home/rafa/Software/BigQuery/API Project-GAP-Telefonica-d0c3f626dc4a.json";
		envi.put("GOOGLE_APPLICATION_CREDENTIALS", "/home/rafa/Software/BigQuery/API Project-GAP-Telefonica-d0c3f626dc4a.json");
		
		System.out.println(envi.toString());
		
		
		String myEnv = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
		System.out.println(myEnv);
		out.println(myEnv + "<br>");
		
		Map<String, String> env = System.getenv();
        for (String envName : env.keySet()) {
            
        	System.out.format("%s=%s%n",
                              envName,
                              env.get(envName));
            out.println(envName );

            out.println(env.get(envName) + "<br>");
            out.println(" ");
        }
        
        
		
		// Instantiates a client
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    ;
		bigquery.getOptions();
		System.out.println("Opciones: " + ServiceOptions.CREDENTIAL_ENV_NAME);
	    System.out.println("Opciones: " + bigquery.getOptions().getCredentials());

	    // The name for the new dataset
	    String datasetName = "my_new_dataset";

	    // Prepares a new dataset
	    Dataset dataset = null;
	    
	    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
	    System.out.println("Data Probando");
	    // Creates the dataset
	    dataset = bigquery.create(datasetInfo);

	   System.out.printf("Dataset %s created.%n", dataset.getDatasetId().getDataset());
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
