package BQ;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class qhrea {

	public qhrea() {
		
	}
	
	 
	
	public static void enviarMail(String destinatario, String asunto, String cuerpo) {
	    // Esto es lo que va delante de @gmail.com en tu cuenta de correo. Es el remitente también.
	     String remitente = "qhrear@gmail.com"; 
	     String clave = "FAYO0173"; //Para la dirección nomcuenta@gmail.com

	    Properties props = System.getProperties();
	    //props.put("mail.smtp.user", remitente);
	    //props.put("mail.smtp.clave", clave);    //La clave de la cuenta
	    
	    props.put("mail.smtp.host", "smtp.gmail.com");  //El servidor SMTP de Google
	    props.put("mail.smtp.auth", "true");    //Usar autenticación mediante usuario y clave
	    props.put("mail.smtp.starttls.enable", "true"); //Para conectar de manera segura al servidor SMTP
	    props.put("mail.smtp.port", "587"); //El puerto SMTP seguro de Google
	    props.put("mail.debug.auth", "true");
	    
	    
	     

	    //Session session = Session.getDefaultInstance(props);
	    //session.setDebug(true);
	    
	    Session session = Session.getInstance(props, new javax.mail.Authenticator() {

	        @Override
			protected PasswordAuthentication getPasswordAuthentication() {
	            return new PasswordAuthentication(remitente,clave);
	        }
	    });
	    
	    MimeMessage message = new MimeMessage(session);

	    try {
	        message.setFrom(new InternetAddress(remitente)); 
	        message.addRecipients(Message.RecipientType.TO, destinatario);//Se podrían añadir varios de la misma manera
	        message.setSubject(asunto);
	        message.setText(cuerpo);
	        message.setContent(cuerpo,"text/html; charset=utf-8");
	        Transport transport = session.getTransport("smtp");
	        transport.connect("smtp.gmail.com", remitente, clave);	        
	        transport.sendMessage(message, message.getAllRecipients());
	        transport.close();
	    }
	    catch (MessagingException me) {
	        me.printStackTrace();   //Si se produce un error
	    }
	}
	
	public int hora() {
		
		Calendar calendario = new GregorianCalendar();
		int hora, minutos, segundos;
		hora =calendario.get(Calendar.HOUR_OF_DAY);
		minutos = calendario.get(Calendar.MINUTE);
		segundos = calendario.get(Calendar.SECOND);
		return hora;
	}
	
	public int minutos() {
		
		Calendar calendario = new GregorianCalendar();
		int hora, minutos, segundos;
		hora =calendario.get(Calendar.HOUR_OF_DAY);
		minutos = calendario.get(Calendar.MINUTE);
		segundos = calendario.get(Calendar.SECOND);
		return minutos;
	}

	public int segundos() {
	
	Calendar calendario = new GregorianCalendar();
	int hora, minutos, segundos;
	hora =calendario.get(Calendar.HOUR_OF_DAY);
	minutos = calendario.get(Calendar.MINUTE);
	segundos = calendario.get(Calendar.SECOND);
	return segundos;
	}
	
	public String horaTotal() {
		
		Calendar calendario = new GregorianCalendar();
		int hora, minutos, segundos;
		hora = calendario.get(Calendar.HOUR_OF_DAY);
		minutos = calendario.get(Calendar.MINUTE);
		segundos = calendario.get(Calendar.SECOND);
		return hora + ":" + segundos + ":" + minutos;
	}
	
public static void main (String [ ] args) throws MessagingException {
	/*
	System.out.println("In progress...");
	enviarMail("rafael.fayosoliver@gmail.com", "Prueba2", "Esto es una prueba");
    System.out.println("Done. OK.");
    */
}

}
