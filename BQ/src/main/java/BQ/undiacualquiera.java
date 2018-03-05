package BQ;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class undiacualquiera
 */
public class undiacualquiera extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public undiacualquiera() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		String fecha = request.getParameter("fecha");
		out.print(fecha);
		try {
			BQ.ActualizarBDGratuito(fecha);
		} catch (TimeoutException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 
		}
		
	}

}
