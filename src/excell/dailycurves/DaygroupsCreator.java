package excell.dailycurves;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import excell.dailycurves.config.Config;
import excell.dailycurves.daygroups.Brueckentag;
import excell.dailycurves.daygroups.Dienstag;
import excell.dailycurves.daygroups.DienstagFerien;
import excell.dailycurves.daygroups.Donnerstag;
import excell.dailycurves.daygroups.DonnerstagFerien;
import excell.dailycurves.daygroups.Feiertag;
import excell.dailycurves.daygroups.Ferienbeginn;
import excell.dailycurves.daygroups.Freitag;
import excell.dailycurves.daygroups.FreitagFerien;
import excell.dailycurves.daygroups.Mittwoch;
import excell.dailycurves.daygroups.MittwochFerien;
import excell.dailycurves.daygroups.Montag;
import excell.dailycurves.daygroups.MontagFerien;
import excell.dailycurves.daygroups.Samstag;
import excell.dailycurves.daygroups.SamstagFerien;
import excell.dailycurves.daygroups.Sonntag;
import excell.dailycurves.daygroups.SonntagFerien;
import excell.dailycurves.daygroups.Tagesgruppe;


public class DaygroupsCreator {
	private int wochentag;
	private boolean istFeiertag;
	private boolean istBrueckentag;
	private boolean istGanztaegigesSonderereignis;
	private boolean sindSchulferien;
	private boolean istFerienbeginn;
	
	private SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd" );
	
		
	public DaygroupsCreator () {
		
	}	
	
	public Tagesgruppe erzeugeNeueTagesgruppeZumDatum(Calendar cal) {
		wochentag = cal.get(Calendar.DAY_OF_WEEK);
				
		pruefeObDatumAufEinenFeiertagFaellt(cal);
		pruefeObDatumAufEinenBrueckentagFaellt(cal);
		pruefeObDatumInDenSchulferienLiegt(cal);
		pruefeObDatumAufEinGanztaegigesSonderereignisFaellt(cal);
		pruefeObDatumAufFerienbeginnFaellt(cal);
		
		return bestimmeRelevanteTagesgruppe();		
	}	
	
	private void pruefeObDatumAufEinenFeiertagFaellt(Calendar cal) {
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:mysql://" + Config.dvm_host + ":" + Config.dvm_port + "/" + Config.dvm_name + "", Config.dvm_user, Config.dvm_pass);
			String sql = "SELECT 1 FROM KAL AS k WHERE k.OKAT=2 AND DATE(k.VON)='"+sdf.format(cal.getTime())+"';";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
				
			istFeiertag=(rs.next()) ? true : false;
		}catch(Exception ex){
			ex.printStackTrace();
		}
		finally{
			try {
       			if (rs != null)
       				rs.close();
       			rs = null;
       		} 
       		catch (Exception ex) {ex.printStackTrace();/* nothing to do */}
       		try {
       			if (st != null)	
       				st.close();
       			st = null;} 
       		catch (Exception ex) {/* nothing to do */}
       		try {
       			if (conn != null) 
       				conn.close();
       			conn = null;} 
       		catch (Exception ex) {/* nothing to do */}
     	  }	
	}
	
	private void pruefeObDatumAufEinenBrueckentagFaellt(Calendar cal) {
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:mysql://" + Config.dvm_host + ":" + Config.dvm_port + "/" + Config.dvm_name + "", Config.dvm_user, Config.dvm_pass);
			String sql = "SELECT 1 FROM KAL AS k WHERE k.OKAT=3 AND DATE(k.VON)='"+sdf.format(cal.getTime())+"'";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
				
			istBrueckentag=(rs.next()) ? true : false;
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		finally{
			try {
       			if (rs != null)
       				rs.close();
       			rs = null;
       		} 
       		catch (Exception ex) {ex.printStackTrace();/* nothing to do */}
       		try {
       			if (st != null)	
       				st.close();
       			st = null;} 
       		catch (Exception ex) {/* nothing to do */}
       		try {
       			if (conn != null) 
       				conn.close();
       			conn = null;} 
       		catch (Exception ex) {/* nothing to do */}
     	  }	

		
	}
	
	private void pruefeObDatumInDenSchulferienLiegt(Calendar cal) {
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:mysql://" + Config.dvm_host + ":" + Config.dvm_port + "/" + Config.dvm_name + "", Config.dvm_user, Config.dvm_pass);
			String sql = "SELECT 1 FROM KAL AS k WHERE k.OKAT=4 AND DATE(k.VON)<='"+sdf.format(cal.getTime())+"' AND DATE(k.BIS)>='"+sdf.format(cal.getTime())+"';";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
				
			sindSchulferien=(rs.next()) ? true : false;
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		finally{
			try {
       			if (rs != null)
       				rs.close();
       			rs = null;
       		} 
       		catch (Exception ex) {ex.printStackTrace();/* nothing to do */}
       		try {
       			if (st != null)	
       				st.close();
       			st = null;} 
       		catch (Exception ex) {/* nothing to do */}
       		try {
       			if (conn != null) 
       				conn.close();
       			conn = null;} 
       		catch (Exception ex) {/* nothing to do */}
     	  }	
	}
	
	private void pruefeObDatumAufEinGanztaegigesSonderereignisFaellt(Calendar cal) {
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:mysql://" + Config.dvm_host + ":" + Config.dvm_port + "/" + Config.dvm_name + "", Config.dvm_user, Config.dvm_pass);
			String sql = "SELECT 1 FROM KAL AS k WHERE k.OKAT=6 AND ((DATE(k.VON)='"+sdf.format(cal.getTime())+"' AND DATE(k.BIS)=DATE(k.VON) " +
						"AND HOUR(k.VON)<=8 and Hour(k.BIS)>=16) " +
						 "OR (DATE(k.VON)<='"+sdf.format(cal.getTime())+"' AND DATE(k.BIS)>='"+sdf.format(cal.getTime())+"' AND DATE(k.VON)<DATE(k.BIS)));";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
				
			istGanztaegigesSonderereignis=(rs.next()) ? true : false;
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		finally{
			try {
       			if (rs != null)
       				rs.close();
       			rs = null;
       		} 
       		catch (Exception ex) {ex.printStackTrace();/* nothing to do */}
       		try {
       			if (st != null)	
       				st.close();
       			st = null;} 
       		catch (Exception ex) {/* nothing to do */}
       		try {
       			if (conn != null) 
       				conn.close();
       			conn = null;} 
       		catch (Exception ex) {/* nothing to do */}
		}	
	}

	
	private void pruefeObDatumAufFerienbeginnFaellt(Calendar cal) {
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:mysql://" + Config.dvm_host + ":" + Config.dvm_port + "/" + Config.dvm_name + "", Config.dvm_user, Config.dvm_pass);
			String sql = "SELECT 1 FROM KAL AS k WHERE k.OKAT=4 AND DATE(k.VON)=DATE_SUB('"+sdf.format(cal.getTime())+"', INTERVAL 1 DAY)";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
				
			istFerienbeginn=(rs.next()) ? true : false;
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
		finally{
			try {
       			if (rs != null)
       				rs.close();
       			rs = null;
       		} 
       		catch (Exception ex) {ex.printStackTrace();/* nothing to do */}
       		try {
       			if (st != null)	
       				st.close();
       			st = null;} 
       		catch (Exception ex) {/* nothing to do */}
       		try {
       			if (conn != null) 
       				conn.close();
       			conn = null;} 
       		catch (Exception ex) {/* nothing to do */}
     	  }	
	}
	
	private Tagesgruppe bestimmeRelevanteTagesgruppe() {
		
		Tagesgruppe tagesgruppe=null;
		
		if(!istGanztaegigesSonderereignis) {
			
			if(istFeiertag) {
				tagesgruppe=new Feiertag();
			} else if(istBrueckentag) {
				tagesgruppe=new Brueckentag();
			} else if(sindSchulferien) {
				
				switch(wochentag) {
				case Calendar.MONDAY:
					tagesgruppe=new MontagFerien();
					break;
				case Calendar.TUESDAY:
					tagesgruppe=new DienstagFerien();
					break;
				case Calendar.WEDNESDAY:
					tagesgruppe=new MittwochFerien();
					break;
				case Calendar.THURSDAY:
					tagesgruppe=new DonnerstagFerien();
					break;
				case Calendar.FRIDAY:
					tagesgruppe=new FreitagFerien();
					break;
				case Calendar.SATURDAY:
					tagesgruppe=new SamstagFerien();
					break;
				case Calendar.SUNDAY:
					tagesgruppe=new SonntagFerien();
					break;
				}
				
			} else if(istFerienbeginn) {
				tagesgruppe=new Ferienbeginn();
				
			} else {
				
				switch(wochentag) {
				case Calendar.MONDAY:
					tagesgruppe=new Montag();
					break;
				case Calendar.TUESDAY:
					tagesgruppe=new Dienstag();
					break;
				case Calendar.WEDNESDAY:
					tagesgruppe=new Mittwoch();
					break;
				case Calendar.THURSDAY:
					tagesgruppe=new Donnerstag();
					break;
				case Calendar.FRIDAY:
					tagesgruppe=new Freitag();
					break;
				case Calendar.SATURDAY:
					tagesgruppe=new Samstag();
					break;
				case Calendar.SUNDAY:
					tagesgruppe=new Sonntag();
					break;
				}	
			}
			
		}
		return tagesgruppe;
	}	
}
