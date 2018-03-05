package BQ;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * Servlet implementation class bqServlet
 */
public class prueba extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public prueba() {
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
		//try {
		//BQ.prueba();
		/*
		String Subject = "Alarma: bajada de leads en GP";
		String Body = "Accede a este enlace para acceder a la alarma: https://generacion-dot-mov-prod3.appspot.com/alarmaServlet";
		qhrea.enviarMail("rafael.fayosoliver@telefonica.com", Subject, Body);
			
		Subject = "Alarma: bajada de leads en Empresas";
		Body = "Accede a este enlace para acceder a la alarma: https://generacion-dot-mov-prod3.appspot.com/alarmaServletEmp";
		qhrea.enviarMail("rafael.fayosoliver@telefonica.com", Subject, Body);
			
		*/
		
				
		/*
		try {
			BQ.QuitarFechaEnFichero("T01_GratuitoYPagado", "2018-01-29");
			BQ.QuitarFechaEnFichero("G01_CDRFiltrado", "2018-01-29");
			BQ.QuitarFechaEnFichero("G04_SesionesUnicas", "20180129");
		} catch (TimeoutException | InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
		
		try {
			
			BQ.AjustarImpresiones();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//BQ.ActualizarBDPerso("20180206");
		//BQ.PonerRegistrosACero("GenerationAlias", "P06_Test", "Leads");
		
		
		
		
		
		
			//try {
				//BQ.QuitarFechaEnFichero("G01_CDRFiltrado", "2018-01-18");
				//BQ.QuitarFechaEnFichero("T01_GratuitoYPagado", "2018-01-18");
				//BQ.QuitarFechaEnFichero("G04_SesionesUnicas", "20180118");
				//BQ.ActualizarBDGratuito("20180129");
				//BQ.ActualizarBDPerso("20180206");
				//BQ.QuitarFechaEnFichero("P01_SQLPerso900", "20180129");
				//BQ.QuitarFechaEnFichero("P01_SQLPersoC2C", "20180129");
				//BQ.ActualizarBDPerso();
			//} catch (InterruptedException | TimeoutException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			//}
			
			
			//BQ.AcumularPerso();
			/*
				String SQL = "SELECT * FROM `mov-prod5.GenerationAlias.T02_GENERACION`";
				String destinationDataset = "GenerationAlias";
				String destinationTable = "Borrame";
				String project = "mov-prod5";
				try {
					BQ.EjecutarConsultaNuevaTabla(SQL, project, destinationDataset, destinationTable);
				} catch (InterruptedException | TimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			*/
		
		//} catch (InterruptedException | TimeoutException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			//out.println(e);
		//}
		out.println("Ok. Operación realizada con éxito");
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
