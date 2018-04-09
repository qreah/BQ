package BQ;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class BQ {

	
	/* Goal: to authenticate into BigQuery Google Cloud. It gets default credentials 
	 * but it is prepared to get credentials from JSON file at the local file system
	 * Input:
	 * 	- None
	 * Output:
	 * 	- BigQuery object with the conexion to manage tables
	 */
	@SuppressWarnings("unused")
	public static BigQuery AuthBQ() throws IOException {
		  // Load credentials from JSON key file. If you can't set the GOOGLE_APPLICATION_CREDENTIALS
		  // environment variable, you can explicitly load the credentials file to construct the
		  // credentials.
		
		/*
		BigQuery bigquery = null;  
		
		
		  GoogleCredentials credentials;
		  File credentialsPath = new File("/home/rafa/eclipse-workspace/BQ/src/main/java/bigquery-prod3.json");  // TODO: update to your key path.
		  if (credentialsPath != null) { 
			  
			  FileInputStream serviceAccountStream = new FileInputStream(credentialsPath) ;
			  credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
			  	bigquery =
			      BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
		  } else {
		  */
		  
		BigQuery  bigquery = BigQueryOptions.getDefaultInstance().getService();
		return bigquery;
		/*
		}  
		  return bigquery;
		  */
		}
		
	
	public static void runQuery(QueryJobConfiguration queryConfig)
	    throws TimeoutException, InterruptedException, IOException {
	  //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		BigQuery bigquery = AuthBQ();
	  // Create a job ID so that we can safely retry.
	  JobId jobId = JobId.of(UUID.randomUUID().toString());
	  Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
	
	  // Wait for the query to complete.
	  queryJob = queryJob.waitFor();
	
	  // Check for errors
	  if (queryJob == null) {
	    throw new RuntimeException("Job no longer exists");
	  } else if (queryJob.getStatus().getError() != null) {
	    // You can also look at queryJob.getStatus().getExecutionErrors() for all
	    // errors, not just the latest one.
	    throw new RuntimeException(queryJob.getStatus().getError().toString());
	  }
	
	  // Get the results.
	  QueryResponse response = bigquery.getQueryResults(jobId);
	
	    TableResult result = queryJob.getQueryResults();
	
	    
	    while (result != null) {
	        for (List<FieldValue> row : result.iterateAll()) {
	          for (FieldValue val : row) {
	            //System.out.printf("%s,", val.toString());
	          }
	          //System.out.printf("\n");
	        }
	
	        result = result.getNextPage();
	    }
	    queryJob.cancel();
	}


	public static void CrearTabla(String NombreTabla, String datasetName) throws IOException {
		
		//Abrir base de datos
		BigQuery bigquery = AuthBQ();
		Dataset dataset = bigquery.getDataset(datasetName);
	    
	    String datasetId = dataset.getDatasetId().getDataset();
	    
		//TableId tableId = TableId.of(datasetId, NombreTabla);
		Field stringField = Field.of("StringField", LegacySQLTypeName.STRING);
		Schema schema = Schema.of(stringField);
		StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
		//bigquery.create(TableInfo.of(tableId, tableDefinition));
		
	}
	
	public static void EjecutarConsultaNuevaTabla(String SQL, String destinationDataset, String destinationTable) throws InterruptedException, TimeoutException, IOException {
		boolean allowLargeResults = true;
		
		QueryJobConfiguration queryConfig = 
		    QueryJobConfiguration.newBuilder(SQL)
		    .setDestinationTable(TableId.of(destinationDataset, destinationTable))
		    .setAllowLargeResults(allowLargeResults)
		    .build();
		runQuery(queryConfig);	
	}
	
	public static void EjecutarConsultaNuevaTabla(String SQL, String Proyecto, String destinationDataset, String destinationTable) throws InterruptedException, TimeoutException, IOException {
		boolean allowLargeResults = true;
		BigQuery bigquery = AuthBQ();
		//BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		
		
		QueryJobConfiguration queryConfig = 
		    QueryJobConfiguration.newBuilder(SQL)
		    .setDestinationTable(TableId.of(Proyecto, destinationDataset, destinationTable))
		    .setAllowLargeResults(allowLargeResults)
		    .build();
		runQuery(queryConfig);	
	}
	
	public static void UpdateTabla(String SQL, String Dataset, String Table) throws TimeoutException, InterruptedException, IOException {
		//boolean allowLargeResults = true;
		QueryJobConfiguration queryConfig = 
		    QueryJobConfiguration.newBuilder(SQL)
		    //.setDestinationTable(TableId.of(Dataset, Table))
		    //.setAllowLargeResults(allowLargeResults)
		    .build();
		runQuery(queryConfig);	
	}
	
	public static void InsertarRegistros(String SQL) throws TimeoutException, InterruptedException, IOException {
		
		QueryJobConfiguration queryConfig = 
		    QueryJobConfiguration.newBuilder(SQL)
		    .build();
		runQuery(queryConfig);	
	}
	
	public static StringBuilder PrintQueryResults(String SQL) throws JobException, InterruptedException, IOException {
		StringBuilder sb = new StringBuilder();
		
		
		QueryJobConfiguration queryConfig =
			      QueryJobConfiguration.newBuilder(SQL).build();
		BigQuery bigquery = AuthBQ();
	
		  // Create a job ID so that we can safely retry.
		  JobId jobId = JobId.of(UUID.randomUUID().toString());
		  Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
	
		  // Wait for the query to complete.
		  queryJob = queryJob.waitFor();
	
		  // Check for errors
		  if (queryJob == null) {
		    throw new RuntimeException("Job no longer exists");
		  } else if (queryJob.getStatus().getError() != null) {
		    // You can also look at queryJob.getStatus().getExecutionErrors() for all
		    // errors, not just the latest one.
		    throw new RuntimeException(queryJob.getStatus().getError().toString());
		  }
		  // Get the results.
		  Long primerCampo = queryJob.getQueryResults().getTotalRows();
		  
		  Schema  Campos = queryJob.getQueryResults().getSchema();
		  
		  	  
		  for (Field val :  Campos.getFields()) {
		    sb.append(val.getName() + ", ");
		   }
		   sb.append('\n');
		   
		  
		  TableResult result = queryJob.getQueryResults();
		  while (result != null) {
			    for (List<FieldValue> row : result.iterateAll()) {
			      for (FieldValue val : row) {
			    	  sb.append(val.getStringValue() + ", ");
			      }
			      sb.append('\n');
			    }
	
			    result = result.getNextPage();
			  }
		  
		  
		  // Print all pages of the results.
		  /*
		  while (result != null) {
		    for (List<FieldValue> row : result.iterateAll()) {
		      for (FieldValue val : row) {
		        System.out.printf("%s,", val.toString());
		      }
		      System.out.printf("\n");
		    }
	
		    result = result.getNextPage();
		  }
		  */
		return sb;
	}


	public static String BorrarTabla(String DatasetName, String NombreTabla) throws IOException {
		String resultado = "";
		BigQuery bigquery = AuthBQ();
		
		TableId tableId = TableId.of(DatasetName, NombreTabla);
		Boolean deleted = bigquery.delete(tableId);
		if (deleted.equals(true)){
			resultado = "OK";
		} else {
			
			resultado = "KO";
		}
		
		return resultado;
	}
	
	public static String BorrarTabla(String Proyecto, String DatasetName, String NombreTabla) throws IOException {
		String resultado = "";
		BigQuery bigquery = AuthBQ();
		TableId tableId = TableId.of(Proyecto, DatasetName, NombreTabla);
		Boolean deleted = bigquery.delete(tableId);
		if (deleted.equals(true)){
			resultado = "OK";
		} else {
			
			resultado = "KO";
		}
		
		return resultado;
	}
	
	public static String SacarDato(String SQL) throws TimeoutException, InterruptedException, IOException {
		String resultado = "";
		QueryJobConfiguration queryConfig = 
			    QueryJobConfiguration.newBuilder(SQL)
			    .build();
		BigQuery bigquery = AuthBQ();
		System.out.println(SQL);
		  // Create a job ID so that we can safely retry.
		  JobId jobId = JobId.of(UUID.randomUUID().toString());
		  Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

		  // Wait for the query to complete.
		  queryJob = queryJob.waitFor();

		  // Check for errors
		  if (queryJob == null) {
		    throw new RuntimeException("Job no longer exists");
		  } else if (queryJob.getStatus().getError() != null) {
		    // You can also look at queryJob.getStatus().getExecutionErrors() for all
		    // errors, not just the latest one.
		    throw new RuntimeException(queryJob.getStatus().getError().toString());
		  }

		  // Get the results.
		  QueryResponse response = bigquery.getQueryResults(jobId);

		    TableResult result = queryJob.getQueryResults();
		    
		    //System.out.println("Número de registros: " + result.getTotalRows());
		    // Print all pages of the results.
		    int i = 1;
		    while (result != null) {
		    	
		        for (List<FieldValue> row : result.iterateAll()) {
		        	
		          for (FieldValue val : row) {
		        	  FieldValue fv = row.get(0);
		        	  String sv; 
		        	  if (fv.isNull()) {
					    	System.out.println("Es null");
					    } else {
					    	System.out.println("No es null");
					    	//System.out.println("Valor: " + fv.getStringValue());
					    	
					    	resultado = fv.getStringValue();
					    	System.out.println("resultado: " + resultado);
					    	
					    	//resultado = row.get(0).getStringValue();
					    }
		            //System.out.printf("%s,", val.toString());
		        	
		        	
		          }
		          //System.out.printf("\n");
		        }

		        result = result.getNextPage();
		    }
		    
		    
		    //for (FieldValueList row : result.iterateAll()) {
		      
		    //  resultado = row.get(0).getStringValue();
		    //}
		  /*
		  //OLD
		  QueryResponse response = bigquery.getQueryResults(jobId);
		  QueryResult result = response.getResult();	
		  System.out.println("result: " + result.toString());
		  
		  while (result != null) {
			    for (List<FieldValue> row : result.iterateAll()) {
			      System.out.printf("row: ", row);
			      //if(row.isEmpty()) {
				      for (FieldValue val : row) {
				        System.out.printf("%s,", val.getStringValue());
				        resultado = resultado + val.getStringValue();
				        System.out.println("Resultado: " + resultado);
				      }
				      System.out.printf("\n");
			      //}
			    }
			    
			    result = result.getNextPage();
			  }
		 System.out.println("Resultado: " + resultado);
		 
		 */
		    
		queryJob.cancel();
		return resultado;
	}
	
	/**
	 * Anexa el resultado de la query 'queryString' en la tabla indicada como destino 
	 * (datasetDest, destTable)
	 * 
	 * @param queryString
	 * @param destinationDataset
	 * @param destinationTable
	 * @param allowLargeResults
	 * @throws TimeoutException
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	public static void appendQueryToTable(String queryString, String destinationDataset, String destinationTable, boolean allowLargeResults) throws TimeoutException, InterruptedException, IOException{
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString)
				.setDestinationTable(TableId.of(destinationDataset, destinationTable))
				.setAllowLargeResults(allowLargeResults).setUseLegacySql(false)
				.setWriteDisposition(WriteDisposition.WRITE_APPEND).build();
		runQuery(queryConfig);
	}


	public static void appendQueryToTable(String queryString, String Project, String destinationDataset, String destinationTable, boolean allowLargeResults) throws TimeoutException, InterruptedException, IOException{
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryString)
				.setDestinationTable(TableId.of(Project, destinationDataset, destinationTable))
				.setAllowLargeResults(allowLargeResults).setUseLegacySql(false)
				.setWriteDisposition(WriteDisposition.WRITE_APPEND).build();
		runQuery(queryConfig);
	}


	public static void AddFechaEnFichero2(String Fichero, String Fecha) throws IOException {
		 
		String path = System.getProperty("user.dir");
		FileWriter writer = new FileWriter(path + "/temp");
		String outline = "";
			
			Reader input = new FileReader(path + "/" + Fichero);
            BufferedReader reader = new BufferedReader(input);
            String line;
            
            while ((line = reader.readLine()) != null) {
            	
                outline = line.replace("XXXX", Fecha);
                //System.out.println(outline);
                writer.append(outline);
                writer.append('\n');
            }
            writer.flush();
            writer.close(); 
            
          File fichero = new File(path + "/" + Fichero);
          File fichero2 = new File(path + "/temp");
          fichero.delete();
          boolean correcto = fichero2.renameTo(fichero);
          //System.out.println(correcto); 
	}
	
	public static void AddFechaEnFichero(String Tabla, String Fecha) throws IOException, TimeoutException, InterruptedException {
		
		String Query = "SELECT SQL FROM `mov-prod5.GenerationAlias.SQLTabla` WHERE Tabla = '" + Tabla + "'";
		String SQL = SacarDato(Query);
		if (SQL == null) {
			System.out.println("Problema");
		} else {
		SQL = SQL.replace("XXXX", Fecha);
		String Update = "UPDATE `mov-prod5.GenerationAlias.SQLTabla` SET SQL = '" + SQL + "' WHERE Tabla = '" + Tabla + "'";
		UpdateTabla(Update, "GenerationAlias", "SQLTabla");
		}
		
	}
	
	public static void QuitarFechaEnFichero2(String Fichero, String Fecha) throws IOException {
		String path = System.getProperty("user.dir");
		FileWriter writer = new FileWriter(path + "/temp");
		String outline = "";
		
			Reader input = new FileReader(path + "/" + Fichero);
            BufferedReader reader = new BufferedReader(input);
            String line;
            
            while ((line = reader.readLine()) != null) {
            	
                outline = line.replace(Fecha, "XXXX");
                //System.out.println(outline);
                writer.append(outline);
                writer.append('\n');
            }
            writer.flush();
            writer.close(); 
            
          File fichero = new File(path + "/" + Fichero);
          File fichero2 = new File(path + "/temp");
          fichero.delete();
          boolean correcto = fichero2.renameTo(fichero);
          //System.out.println(correcto); 
		}
	
	public static void QuitarFechaEnFichero(String Tabla, String Fecha) throws IOException, TimeoutException, InterruptedException {
		String Query = "SELECT SQL FROM `mov-prod5.GenerationAlias.SQLTabla` WHERE Tabla = '" + Tabla + "'";
		String SQL = SacarDato(Query);
		SQL = SQL.replace(Fecha, "XXXX");
		if (SQL != null) {
			String Update = "UPDATE `mov-prod5.GenerationAlias.SQLTabla` SET SQL = '" + SQL + "' WHERE Tabla = '" + Tabla + "'";
			UpdateTabla(Update, "GenerationAlias", "SQLTabla");
		} else {
			System.out.println("Error: el SELECT está vacío");
		}
		
	}
	
	//Extrae un String con la consulta SQL que en en la Tabla SQLTabla
	public static String SQL2(String Fichero) throws IOException {
		String path = System.getProperty("user.dir");
		String sql = "";
		Reader input = new FileReader(path + "/" + Fichero);
        BufferedReader reader = new BufferedReader(input);
        String line;
        
        while ((line = reader.readLine()) != null) {
        	
        	sql = sql + line + "\n";            
        }
        //System.out.println(sql);
		return sql;
	}
	
	public static String SQL(String Tabla) throws IOException, TimeoutException, InterruptedException {
		String Query = "SELECT SQL FROM `mov-prod5.GenerationAlias.SQLTabla` WHERE Tabla = '" + Tabla + "'";
		String sql = SacarDato(Query); 
		return sql;
	}
	
	// Inputs
	// Fecha tiene el formato 20180129
	
	public static void ActualizarBDGratuito(String Fecha) throws IOException, TimeoutException, InterruptedException {
		
		String Dataset = "GenerationAlias";
		String Project = "mov-prod5";
		
		//Incluir las fechas en los ficheros SQL 
		String Fecha2 = Fecha.substring(0, 4) + "-" + Fecha.substring(4, 6)  + "-" + Fecha.substring(6,8);
		
		AddFechaEnFichero("G01_CDRFiltrado", Fecha2);
		AddFechaEnFichero("T01_GratuitoYPagado", Fecha2);
		AddFechaEnFichero("G04_SesionesUnicas", Fecha);
		
		// To create Table G01_CDRFiltrado (CDR with lead criteria) with the new date
		
		String Tabla1 = "G01_CDRFiltrado";
		String SQL1 = SQL(Tabla1);
		
				
		try {
			
			String Estado = BQ.BorrarTabla(Project, Dataset, Tabla1);
		
			if (Estado.equals("OK")) {
				System.out.println("Hecho. OK. La Tabla " + Tabla1 + " se ha borrado correctamente");
			} else {
				System.out.println("La Tabla " + Tabla1 + " no existía");
			}
			BQ.EjecutarConsultaNuevaTabla(SQL1, Project, Dataset, Tabla1);
			
		} catch (InterruptedException | TimeoutException e) {
			System.out.println("Ha ocurrido un error");
			e.printStackTrace();
		}
		
		// To create Table G04_SesionesUnicas (GA information) with the new date
		
		String Tabla2 = "G04_SesionesUnicas";
		String SQL2 = SQL(Tabla2);
		System.out.println("G04_SesionesUnicas = " + SQL2);		
		try {
			
			String Estado = BQ.BorrarTabla(Project, Dataset, Tabla2);
		
			if (Estado.equals("OK")) {
				System.out.println("Hecho. OK. La Tabla " + Tabla2 + " se ha borrado correctamente");
			} else {
				System.out.println("La Tabla " + Tabla2 + " no existía");
			}
			BQ.EjecutarConsultaNuevaTabla(SQL2, Project, Dataset, Tabla2);
			
		} catch (InterruptedException | TimeoutException e) {
			System.out.println("Ha ocurrido un error");
			e.printStackTrace();
		}
		
		
		
		//G09_FinalG: base de datos donde se hace un Update para crear los campos TipoGeneracion,
		//CTA y Area
		
		String SQLFinal = "SELECT DATE,PageName,PN, Soporte_COL, Visitas, Leads, CTA, Area, TipoGeneracion FROM `mov-prod5.GenerationAlias.G08_FinalGratuito`";
		String Tabla = "G09_FinalG";
		try {
			
			String Estado = BQ.BorrarTabla(Project, Dataset, Tabla);
		
			if (Estado.equals("OK")) {
				System.out.println("Hecho. OK. La Tabla " + Tabla + " se ha borrado correctamente");
			} else {
				System.out.println("La Tabla " + Tabla + " no existía");
			}
			BQ.EjecutarConsultaNuevaTabla(SQLFinal, Project, Dataset, Tabla);
			String SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.TipoGeneracion = 'GRATUITO' WHERE (BD.PageName IS NULL OR BD.PageName IS NOT NULL)";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.TipoGeneracion = 'PAGADO' WHERE BD.Soporte_COL = 'COMPARADORES' OR BD.Soporte_COL = 'REDES SOCIALES' OR BD.Soporte_COL = 'COMMUNITY' OR BD.Soporte_COL = 'SEM EXTERNO' OR BD.Soporte_COL = 'TELEVISION' OR BD.Soporte_COL = 'DISPLAY' OR BD.Soporte_COL = 'RMK'";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.TipoGeneracion = 'AMBOS' WHERE BD.Soporte_COL = 'proyectopymes-gp' OR BD.Soporte_COL = 'TODAS CAMPAÑAS' OR BD.Soporte_COL = ''";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.CTA = 'Sin CTAs' WHERE (BD.PageName IS NULL AND BD.PN IS NOT NULL) OR BD.PageName = 'No tiene' OR BD.PageName = 'Proceso de compra No tiene' OR BD.PageName = 'SEO no tiene'";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.CTA = 'Con CTAs' WHERE BD.PageName IS NOT NULL";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.Area = 'HOME' WHERE REGEXP_CONTAINs(BD.PageName, r'home')";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.Area = 'TV' WHERE REGEXP_CONTAINs(BD.PageName, r'fusion')";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.Area = 'BAF' WHERE REGEXP_CONTAINs(BD.PageName, r'internet')";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.Area = 'MOVIL' WHERE REGEXP_CONTAINs(BD.PageName, r'movil')";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.Area = 'TV' WHERE REGEXP_CONTAINs(BD.PageName, r'tv')";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.G09_FinalG` AS BD SET BD.Area = 'OTROS' WHERE REGEXP_CONTAINs(BD.PageName, r'home') = FALSE AND REGEXP_CONTAINs(BD.PageName, r'fusion') = FALSE AND REGEXP_CONTAINs(BD.PageName, r'internet') = FALSE AND REGEXP_CONTAINs(BD.PageName, r'tv') = FALSE AND REGEXP_CONTAINs(BD.PageName, r'movil') = FALSE";
			UpdateTabla(SQLUpdate, Dataset, Tabla);
			
			
		} catch (InterruptedException | TimeoutException e) {
			System.out.println("Ha ocurrido un error");
			e.printStackTrace();
		}
			//T01_GratuitoYPagado: Añadir TipoGeneracion, Area y CTAs a SEM INTERNO
			
			Tabla = "T01_GratuitoYPagado";
			try {
				
				String SQL_T01_GratuitoYPagado = SQL(Tabla);
				String Estado = BQ.BorrarTabla("mov-prod5", "GenerationAlias", Tabla);
			
				if (Estado.equals("OK")) {
					System.out.println("Hecho. OK. La Tabla " + Tabla + " se ha borrado correctamente");
				} else {
					System.out.println("La Tabla " + Tabla + " no existía");
				}
				BQ.EjecutarConsultaNuevaTabla(SQL_T01_GratuitoYPagado, Project, Dataset, Tabla);
				String SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.T01_GratuitoYPagado` AS BD SET BD.TipoGeneracion = 'PAGADO' WHERE BD.CampaignId IS NOT NULL";
				UpdateTabla(SQLUpdate, Dataset, Tabla);
				SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.T01_GratuitoYPagado` AS BD SET BD.CTA = 'Con CTAs' WHERE BD.CampaignId IS NOT NULL";
				UpdateTabla(SQLUpdate, Dataset, Tabla);
				SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.T01_GratuitoYPagado` AS BD SET BD.Area = 'TV' WHERE REGEXP_CONTAINS(BD.Producto, r'FUSION')";
				UpdateTabla(SQLUpdate, Dataset, Tabla);
				SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.T01_GratuitoYPagado` AS BD SET BD.Area = 'BAF' WHERE REGEXP_CONTAINS(BD.Producto, r'BAF')";
				UpdateTabla(SQLUpdate, Dataset, Tabla);
				SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.T01_GratuitoYPagado` AS BD SET BD.Area = 'MOVIL' WHERE REGEXP_CONTAINS(BD.Producto, r'MOVIL')";
				UpdateTabla(SQLUpdate, Dataset, Tabla);
				SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.T01_GratuitoYPagado` AS BD SET BD.Soporte_COL = 'SEM INTERNO' WHERE BD.CampaignId IS NOT NULL";
				UpdateTabla(SQLUpdate, Dataset, Tabla);
				
			
			//Acumular para tener un histórico
			
			String SQLAcum = "INSERT `mov-prod5.GenerationAlias.T02_GENERACION` (DATE, Demanda, Leads, RE, Producto, Territorio, CampaignName, ExternalCustomerId, CampaignId, Clicks, Cost, CPC, AvgPosition, CTR, LTR, Cliente, Cuenta, ProductoBuscado, PageName, Soporte_COL, CTA, Area, TipoGeneracion) SELECT * FROM `mov-prod5.GenerationAlias.T01_GratuitoYPagado`";
			BQ.InsertarRegistros(SQLAcum);
			
		} catch (InterruptedException | TimeoutException e) {
			System.out.println("Ha ocurrido un error");
			e.printStackTrace();
		}
	
		QuitarFechaEnFichero("G01_CDRFiltrado", Fecha2);
		QuitarFechaEnFichero("T01_GratuitoYPagado", Fecha2);
		QuitarFechaEnFichero("G04_SesionesUnicas", Fecha);
			
	
	}
	
	/*	Goal: tasks to create the tables and processing the information needed to 
	 * 	get the information about lead generation at the website
	 * 	Input:
	 * 		- None
	 * 	Output:
	 * 		- None
	 * 
	 */
		
	public static void ProcesarGratuito() throws ParseException, TimeoutException, InterruptedException, IOException {
		
		
		//Procesing information about Dates in order to check if Tables need to be updated or not
		//It all depends if CDR and Adwords information area updated
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		String MAXDATE_Adwords = SacarDato("SELECT MAX(DATE) FROM `mov-prod5.GenerationAlias.AdwordsCampaigns`");
		String MAXDATE_Generacion = SacarDato("SELECT MAX(DATE) FROM `mov-prod5.GenerationAlias.T02_GENERACION`");
		DateFormat dfMAXDATE_Adwords = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat dfMAXDATE_Generacion = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calMAXDATE_Adwords  = Calendar.getInstance();	
		calMAXDATE_Adwords.setTime(dfMAXDATE_Adwords.parse(MAXDATE_Adwords));
		Calendar calMAXDATE_Generacion  = Calendar.getInstance();
		calMAXDATE_Generacion.setTime(dfMAXDATE_Generacion.parse(MAXDATE_Generacion));
		int comp = calMAXDATE_Generacion.compareTo(calMAXDATE_Adwords);
		
		// If comp = 0 it is that evertything is updated
		
		if (comp == 0) {
			String Asunto = "Actualización base de datos";
			String Cuerpo = "Se ha intentado actualizar la base datos pero ya está actualizada"; 
			qhrea.enviarMail("rafael.fayosoliver@telefonica.com", Asunto, Cuerpo);
			
		} else  {
			
			// If comp > 0 it is not a posibility because Gratuito information must be updated
			// after CDR and Adwords are updated, not before. There is a mistake
			
			if (comp > 0) {
				qhrea q = new qhrea();
				String Asunto = "Error en las fechas de las bases de datos";
				String Cuerpo = "La fecha de la tabla de Adwords (AdwordsCampaigns) es anterior a la de Generacion (T02_GENERACION): " + MAXDATE_Adwords + " vs. " + MAXDATE_Generacion;
				qhrea.enviarMail("rafael.fayosoliver@telefonica.com", Asunto, Cuerpo);
				
				
			// If comp < 0, the process must continue because there are som days ready 
			// to be updated
				
			} else {
				
				long dif = calMAXDATE_Adwords.getTimeInMillis()-calMAXDATE_Generacion.getTimeInMillis();
				dif = dif / (24 * 60 * 60 * 1000);
				//System.out.println("Adwords Date: " + MAXDATE_Adwords);
				//System.out.println("dif: " + dif);
				for (int i = 1; i <= dif; i++) {
					Calendar calendario = calMAXDATE_Generacion;
					calendario.add(Calendar.DAY_OF_MONTH, 1);
					String strdate = df.format(calendario.getTime());
					//System.out.println("Fechas a procesar: " + strdate);
					strdate = strdate.replace("-", "");
					//System.out.println("Fechas a procesar: " + strdate);
					qhrea q = new qhrea();
					String Asunto = "Actualización Base de Datos Generación";
					String Cuerpo = "La base de datos ya se ha acatualizado. Se ha(n) actualizado" + dif + "día(s)";
					qhrea.enviarMail("rafael.fayosoliver@telefonica.com", Asunto, Cuerpo);
					//qhrea.enviarMail("laura.arangorodriguez@telefonica.com", Asunto, Cuerpo);
					//qhrea.enviarMail("pablo.cerrolazamenendez@telefonica.com", Asunto, Cuerpo);
					//qhrea.enviarMail("alvaro.ferreiradiaz@telefonica.com", Asunto, Cuerpo);
					//qhrea.enviarMail("begona.delgadopena@telefonica.com", Asunto, Cuerpo);
					//qhrea.enviarMail("laura.avilesfiguerola@telefonica.com", Asunto, Cuerpo);
					ActualizarBDGratuito(strdate);
					ActualizarBDPerso(strdate);
				}
				
			}
		}
	
	
	}
	
	/*	Goal: tasks to create the tables and processing the information needed to 
	 * 	get the information about lead generation at the website
	 * 	Input:
	 * 		- None
	 * 	Output:
	 * 		- None
	 * 
	 */
		
	public static void ActualizarBDPerso(String Fecha) throws IOException, TimeoutException, InterruptedException {
		
		//TODO revisar las consultas de la tabla SQLTabla
		String Dataset = "GenerationAlias";
		String Project = "mov-prod5";
		AddFechaEnFichero("P01_SQLPerso900", Fecha);
		AddFechaEnFichero("P01_SQLPersoC2C", Fecha);
		String Fecha2 = Fecha.substring(0, 4) + "-" + Fecha.substring(4, 6)  + "-" + Fecha.substring(6,8);
		
		AddFechaEnFichero("P00_CDR", Fecha2);
		
		
		String TablaCDR = "P00_CDR";
		BQ.BorrarTabla(Project, Dataset, TablaCDR);
		String SQLCDR = SQL(TablaCDR);
		System.out.println("SQL Perso: " + SQLCDR);
		BQ.EjecutarConsultaNuevaTabla(SQLCDR, Project, Dataset, TablaCDR); 
		
		String Tabla900 = "P01_SQLPerso900";
		BQ.BorrarTabla(Project, Dataset, Tabla900);
		String SQL900 = SQL(Tabla900);
		//System.out.println("SQL Perso: " + SQL900);
		BQ.EjecutarConsultaNuevaTabla(SQL900, Project, Dataset, Tabla900);
		
		String TablaC2C = "P01_SQLPersoC2C";
		BQ.BorrarTabla(Project, Dataset, TablaC2C);
		String SQLC2C = SQL(TablaC2C); 
		BQ.EjecutarConsultaNuevaTabla(SQLC2C, Project, Dataset, TablaC2C);
		
		/*
		String TablaPersoCDR = "P03_PersoyCDR";
		BQ.BorrarTabla(Project, Dataset, TablaPersoCDR);
		String SQLPersoCDR = SQL(TablaPersoCDR); 
		BQ.EjecutarConsultaNuevaTabla(SQLPersoCDR, Project, Dataset, TablaPersoCDR);
		*/
		
		String TablaCDRyGA = "P06_CDR_GA";
		BQ.BorrarTabla(Project, Dataset, TablaCDRyGA);
		String SQLCDRyGA = SQL(TablaCDRyGA); 
		BQ.EjecutarConsultaNuevaTabla(SQLCDRyGA, Project, Dataset, TablaCDRyGA);
		
		/*
		String TablaGA = "P06_GA";
		BQ.BorrarTabla(Project, Dataset, TablaGA);
		String SQLGA = SQL(TablaGA); 
		BQ.EjecutarConsultaNuevaTabla(SQLGA, Project, Dataset, TablaGA);
		*/
		
		// TODO Poner todas estas funciones que dependan sólo de la fecha
		// TODO asegurar que todas las tablas tienen la fecha adecuada
		BQ.AjustarImpresiones();
		BQ.PonerRegistrosACero("GenerationAlias", "P06_CDR_GA", "Leads");
		
		BQ.GestionarIncidencia(Fecha);
		
		
		String SQLAcum = "INSERT `mov-prod5.GenerationAlias.T02_PERSONALIZACION` (DATE, Impresiones, Leads, Alias_COL, alias, Action, Label, Pers, Segmento, Territorio, targetAud, productoComunicado, planta, testAB, Soporte_COL) SELECT * FROM `mov-prod5.GenerationAlias.P07_PersoFinal`";
		BQ.InsertarRegistros(SQLAcum);
		
		
		
		QuitarFechaEnFichero("P01_SQLPerso900", Fecha);
		QuitarFechaEnFichero("P01_SQLPersoC2C", Fecha);
		QuitarFechaEnFichero("P00_CDR", Fecha2);
		
		
	}

	
	public static void GestionarIncidencia(String Fecha) throws TimeoutException, InterruptedException, IOException {
		
		String Dataset = "GenerationAlias";
		String Project = "mov-prod5"; 
		
		String TablaIncidencia = "P06_Incidencia";
		BQ.BorrarTabla(Project, Dataset, TablaIncidencia);
		String SQLIncidencia = SQL(TablaIncidencia); 
		BQ.EjecutarConsultaNuevaTabla(SQLIncidencia, Project, Dataset, TablaIncidencia);
		
		//Crear registro del día con incidencia y los leads asociados y borrar
		//los leads de los registros con los alias 900104752 y 900104752-C2C
		String SQLIncidencia900104752 = "SELECT DISTINCT Leads FROM `mov-prod5.GenerationAlias.P06_Incidencia` WHERE Alias = '900104752'";
		//System.out.println("SQLIncidencia900104752: " + SQLIncidencia900104752);
		
		String LeadsIncidencia900104752 = SacarDato(SQLIncidencia900104752);
		if (LeadsIncidencia900104752=="") {
			LeadsIncidencia900104752= "0";
			System.out.println("LeadsIncidencia900104752_null: " + LeadsIncidencia900104752);
		} else {
			LeadsIncidencia900104752 = SacarDato(SQLIncidencia900104752);
			System.out.println("LeadsIncidencia900104752: " + LeadsIncidencia900104752);
		}
		
		String SQLIncidencia900104752C2C = "SELECT DISTINCT Leads FROM `mov-prod5.GenerationAlias.P06_Incidencia` WHERE Alias = '900104752-C2C'";
		
		String LeadsIncidencia900104752C2C;
		if (SacarDato(SQLIncidencia900104752C2C) == ""){
			LeadsIncidencia900104752C2C= "0";
		} else {
			LeadsIncidencia900104752C2C = SacarDato(SQLIncidencia900104752C2C);
		}
		
		
		String SQLUpdate = "UPDATE `mov-prod5.GenerationAlias.P06_Incidencia` " + 
				"SET Leads = 0 " + 
				"WHERE Alias = '900104752' OR Alias = '900104752-C2C'";
		UpdateTabla(SQLUpdate, Dataset, "P06_Incidencia"); 
				
		//System.out.println("LeadsIncidencia: " + LeadsIncidencia);
		/*
		String SQLInsert = "INSERT `mov-prod5.GenerationAlias.P06_Error` (date, Label, Leads) "
			+ "VALUES ('" 
			+ Fecha 
			+ "', 'Incidencia', " + LeadsIncidencia + ")";
		*/
				
		String SQLInsert1 = "INSERT `mov-prod5.GenerationAlias.P06_Incidencia` (DATE, Impresiones, Leads, Alias_COL, alias, Action, Label, Pers, Segmento, Territorio, targetAud, productoComunicado, planta, testAB, Soporte_COL) VALUES('" + Fecha + "', 0, " + LeadsIncidencia900104752 + ", '900104752', '900104752', 'Incidencia', 'Incidencia 900104752', '','', '', '', '', '', '', '')";
		System.out.println("SQLInsert1: " + SQLInsert1);
		UpdateTabla(SQLInsert1, Dataset, "P06_Incidencia");
		String SQLInsert2 = "INSERT `mov-prod5.GenerationAlias.P06_Incidencia` (DATE, Impresiones, Leads, Alias_COL, alias, Action, Label, Pers, Segmento, Territorio, targetAud, productoComunicado, planta, testAB, Soporte_COL) VALUES('" + Fecha + "', 0, " + LeadsIncidencia900104752C2C + ", '900104752-C2C', '900104752-C2C', 'Incidencia', 'Incidencia 900104752-C2C', '','', '', '', '', '', '', '')";
		
		UpdateTabla(SQLInsert2, Dataset, "P06_Incidencia");
	}

	
	public static void AjustarImpresiones() throws IOException, InterruptedException {
		String Dataset = "GenerationAlias";
		String Project = "mov-prod5";
		String Tabla = "P06_CDR_GA";
		String SQL = "SELECT * FROM `mov-prod5.GenerationAlias.P06_CDR_GA`" + 
				" ORDER BY Alias_COL";
		String Update = "UPDATE `mov-prod5.GenerationAlias.P06_CDR_GA` SET Impresiones = CASE Alias_COL ";
		String ListaAlias = "WHERE Alias_COL IN (";
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(SQL).build();
		BigQuery bigquery = AuthBQ();
		JobId jobId = JobId.of(UUID.randomUUID().toString());
		Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
		queryJob = queryJob.waitFor();
		// Check for errors
		if (queryJob == null) {
		throw new RuntimeException("Job no longer exists");
		} else if (queryJob.getStatus().getError() != null) {
		  throw new RuntimeException(queryJob.getStatus().getError().toString());
		}
		QueryResponse response = bigquery.getQueryResults(jobId);
		TableResult result = queryJob.getQueryResults();
		//String Label1 = "pers_PA_WEB-OfertasMovistar_IP-Competencia_todos-productos_900500690_900500690-C2C_Generico-NoCliente_-";
		String LabelAnterior = "dnfewiofncwsssssssss";
		List<FieldValue> rowAnterior = new ArrayList<FieldValue>();
		int i = 0;
		float NewImpresiones;
		float NewImpresionesAnterior;
		
		while (result != null) {
			for (List<FieldValue> row : result.iterateAll()) {
					
				//LabelAnterior = rowAnterior.get(6).getStringValue();
					if (i !=0) {
				
		            //System.out.printf("%s,", val.toString());
					String Label = row.get(6).getStringValue();
					Long Impresiones = row.get(1).getLongValue();
					Long Leads = row.get(2).getLongValue();
					
					
					Long LeadsAnterior = rowAnterior.get(2).getLongValue();
					if (Label.equals(rowAnterior.get(6).getStringValue())) {
						if (Impresiones.equals(rowAnterior.get(1).getLongValue())) {
							
							
							NewImpresiones = (float) ((float)(Leads/((float)Leads+(float)LeadsAnterior))*(float)Impresiones*2);
							NewImpresiones = Math.round((double)NewImpresiones);
							String NewImpresionesS = String.valueOf(NewImpresiones);
							NewImpresionesS = NewImpresionesS.substring(0, NewImpresionesS.length()-2);
							
							NewImpresionesAnterior = (float) (((float)LeadsAnterior/((float)Leads+(float)LeadsAnterior))*(float)Impresiones*2);
							NewImpresionesAnterior = Math.round((double)NewImpresionesAnterior);
							String NewImpresionesAnteriorS = String.valueOf(NewImpresionesAnterior);
							NewImpresionesAnteriorS = NewImpresionesAnteriorS.substring(0, NewImpresionesAnteriorS.length()-2);
							
							Update = Update + "WHEN '" + rowAnterior.get(3).getStringValue() + "' THEN " + NewImpresionesAnteriorS + " ";
							ListaAlias = ListaAlias + " '" + rowAnterior.get(3).getStringValue() + "', ";
							Update = Update + "WHEN '" + row.get(3).getStringValue() + "' THEN " + NewImpresionesS + " ";
							ListaAlias = ListaAlias + " '" + row.get(3).getStringValue() + "', ";
						}
					}
					} 
				i++;
				rowAnterior = row;
				Long LeadsAnterior = rowAnterior.get(2).getLongValue();
				
				Long ImpresionesAnterior = rowAnterior.get(1).getLongValue();
		          //System.out.printf("\n");
		    }

		    result = result.getNextPage();
		}
		
		ListaAlias = ListaAlias.substring(0, ListaAlias.length()-2);
		Update = Update + " END " + ListaAlias + ")";
		
		
		
	}
	
	public static void ActualizarBDPerso2() throws InterruptedException, TimeoutException, IOException {
		String Dataset = "GenerationAlias";
		String TablaPerso = "Perso";
		String TablaPersoFinal = "PersoFinal";
		BQ.BorrarTabla(Dataset, TablaPersoFinal);
		
		
		// Gestión de tablas intermedias (TablaPersoC2C y TablaPerso900) para gestionar
		// los alias de los 900s y los C2C
		String TablaPersoC2C = "PersoC2C";
		InputStream in = BQ.class.getResourceAsStream("/SQLPersoC2C");
		String SQLPersoC2C = CharStreams.toString(new InputStreamReader(
			      in, Charsets.UTF_8));
		BQ.EjecutarConsultaNuevaTabla(SQLPersoC2C, Dataset, TablaPersoC2C);
		String TablaPerso900 = "Perso900";
		in = BQ.class.getResourceAsStream("/SQLPerso900");
		String SQLPerso900 = CharStreams.toString(new InputStreamReader(
			      in, Charsets.UTF_8));
		BQ.EjecutarConsultaNuevaTabla(SQLPerso900, Dataset, TablaPerso900);
		
		// Unificar tablas
		in = BQ.class.getResourceAsStream("/SQLPersoUnion");
		String SQLPersoUnion = CharStreams.toString(new InputStreamReader(
			      in, Charsets.UTF_8));
		BQ.EjecutarConsultaNuevaTabla(SQLPersoUnion, Dataset, TablaPerso);
		BQ.BorrarTabla(Dataset, TablaPersoC2C);
		BQ.BorrarTabla(Dataset, TablaPerso900);
		
		
		// BD Final donde se cruza la tabla Perso con la del CDR
		in = BQ.class.getResourceAsStream("/SQLPersoFinal");
		String SQLPersoFinal = CharStreams.toString(new InputStreamReader(
			      in, Charsets.UTF_8));
		BQ.EjecutarConsultaNuevaTabla(SQLPersoFinal, Dataset, TablaPersoFinal);
		
		//Borrar: sólo para sacar el histórico
		//String SQLTemp = "SELECT * FROM `mov-prod3.GenerationAlias.PersoFinal`";
		//BQ.EjecutarConsultaNuevaTabla(SQLTemp, Dataset, "PersoTemp");
		
		BQ.BorrarTabla(Dataset, TablaPerso);
		
		//Crear registro del día con incidencia y los leads asociados y borrar
		//los leads de los registros con los alias 900104752 y 900104752-C2C
		String SQLIncidencia = "SELECT SUM(Leads) FROM `mov-prod3.GenerationAlias.PersoFinal` WHERE Alias = '900104752' OR Alias = '900104752-C2C'";
		String LeadsIncidencia = SacarDato(SQLIncidencia);
		
		String SQLInsert = "INSERT `mov-prod3.GenerationAlias.PersoFinal` (date, Label, Leads) "
				+ "VALUES (" 
				//+ "FORMAT_DATE(\"%Y%m%d\", DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))"
				+ "'20180206'"
				+ ", 'Incidencia', " + LeadsIncidencia + ")";
		
		UpdateTabla(SQLInsert, Dataset, TablaPersoFinal);
		String SQLUpdate = "UPDATE `mov-prod3.GenerationAlias.PersoFinal` " + 
				"SET Leads = 0 " + 
				"WHERE Alias = '900104752' OR Alias = '900104752-C2C'";
		UpdateTabla(SQLUpdate, Dataset, TablaPersoFinal); 
		
	}

	/*
	 * 	Mira alias que sean erróneos porque para varios Label tienen el mismo alias
	 * 	y pone los leads a cero menos el que tiene más impresiones
	 * 	Input:
	 * 		- Dataset: Dataset de trabajo
	 * 		- Tabla: Tabla donde se produce el error (P06_AUX)
	 * 		- Campo
	 * 	Output:
	 * 		- none
	 */
	

	public static void PonerRegistrosACero(String Dataset, String Tabla, String Campo) throws TimeoutException, InterruptedException, IOException {
	List<String> ListaAliasError = new ArrayList<String>();
	ListaAliasError = aliasErroneos("P06_AUX");
	
	
	
	int i = 0;
	
	while (i < ListaAliasError.size()) {
		
		
		
		String SQLSELECT = "SELECT Label FROM  `mov-prod5." + Dataset + "." + Tabla + "` " + 
				"WHERE Alias = '" + ListaAliasError.get(i) + "' ORDER BY Impresiones DESC LIMIT 1";
		
		String Label = BQ.SacarDato(SQLSELECT);
		
		String SQLUpdate = "UPDATE `mov-prod5." + Dataset + "." + Tabla + "` " + 
				"SET " + Campo + " = 0 " + 
				"WHERE Alias = '" + ListaAliasError.get(i) + "' AND Label NOT LIKE '" + Label + "'";
		
		UpdateTabla(SQLUpdate, Dataset, Tabla); 
		i++;
		
	}
	}
	
	public static List<String> aliasErroneos(String Tabla) throws TimeoutException, InterruptedException, IOException {
		String aliasErroneo = "";
		String SQL = "SELECT * FROM `mov-prod5.GenerationAlias." + Tabla + "`";
		List<String> ListaAliasError = new ArrayList<String>();
		QueryJobConfiguration queryConfig = 
			    QueryJobConfiguration.newBuilder(SQL)
			    .build();
		BigQuery bigquery = AuthBQ();
	
		  // Create a job ID so that we can safely retry.
		  JobId jobId = JobId.of(UUID.randomUUID().toString());
		  Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
	
		  // Wait for the query to complete.
		  queryJob = queryJob.waitFor();
	
		  // Check for errors
		  if (queryJob == null) {
		    throw new RuntimeException("Job no longer exists");
		  } else if (queryJob.getStatus().getError() != null) {
		    // You can also look at queryJob.getStatus().getExecutionErrors() for all
		    // errors, not just the latest one.
		    throw new RuntimeException(queryJob.getStatus().getError().toString());
		  }
	
		  // Get the results.
		  QueryResponse response = bigquery.getQueryResults(jobId);
	
		    TableResult result = queryJob.getQueryResults();
		    String aliasErroneoAnterior = "";
		    // Print all pages of the results.
		    while (result != null) {
		        for (List<FieldValue> row : result.iterateAll()) {
		          for (FieldValue val : row) {
		        	if (Integer.parseInt(row.get(5).getStringValue())>=2) {
		        		aliasErroneo = row.get(4).getStringValue();
		        		if (aliasErroneo.equals(aliasErroneoAnterior)) {
			        		
		        		} else {
		        			
			        		aliasErroneoAnterior = aliasErroneo;
				            ListaAliasError.add(aliasErroneo);
				            
		        			
		        		}
		        	}
		            
		          }
		          
		        }
	
		        result = result.getNextPage();
		    }
		
		
		return ListaAliasError;
	}


	public static void FixTablePerso() {
		
		// Comprobar si la tabla P06_CDR_GA tiene errores
		String FixSQL = "SELECT * FROM `mov-prod5.GenerationAlias.P06_AUX`";
		
		// Poner los leads de los registros con errores a cero
		
		// Enviar correo electrónico con los errores
	}
	
	
	
	/*	Goal: tasks to create the tables and processing the information needed to 
	 * 	get the information about lead generation at the website
	 * 	Input:
	 * 		- None
	 * 	Output:
	 * 		- None
	 * 
	 */
		
	public static void AcumularPerso() throws IOException, TimeoutException, InterruptedException {
		ActualizarBDPerso2();
		String SQL = "SELECT * FROM `mov-prod3.GenerationAlias.PersoFinal`";
		appendQueryToTable(SQL, "GenerationAlias", "Personalizacion", true);
		appendQueryToTable(SQL, "mov-prod5", "GenerationAlias", "Personalizacion", true);
		
		
	}


	public static void ActualizarBDGeneracion() throws IOException {
		InputStream in = BQ.class.getResourceAsStream("/SQLGeneracion");
		String SQL = CharStreams.toString(new InputStreamReader(
			      in, Charsets.UTF_8));
		try {
			String Tabla = "Generacion";
			String Estado = BQ.BorrarTabla("GenerationAlias", Tabla);
			if (Estado.equals("OK")) {
				System.out.println("Hecho. OK. La Tabla " + Tabla + " se ha borrado correctamente");
			} else {
				System.out.println("La Tabla " + Tabla + " no existía");
			}
			BQ.EjecutarConsultaNuevaTabla(SQL, "GenerationAlias", Tabla);
			
		} catch (InterruptedException | TimeoutException e) {
			System.out.println("Ha ocurrido un error procesando Tabla Generacion");
			e.printStackTrace();
		}
	}
	
	public static StringBuilder AlarmaGP() throws JobException, InterruptedException, IOException {
		StringBuilder sb = null;
		String SQL = "SELECT * FROM `mov-prod5.GenerationAlias.Alarma01_BajadaAlias`";
		sb = PrintQueryResults(SQL);
		return sb;
	}
	
	public static StringBuilder AlarmaEmpresas() throws JobException, InterruptedException, IOException {
		StringBuilder sb = null;
		String SQL = "SELECT * FROM `mov-prod5.GenerationAlias.Alarma01_BajadaAlias_Emp`";
		sb = PrintQueryResults(SQL);
		return sb;
	}
	
	public static void main(String[] args) {
		

	}

}
