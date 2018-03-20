package excell.dailycurves.achiv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import excell.dailycurves.Function;
import excell.dailycurves.config.Config;

public class ArchivingEdgeRecords {
	
	private SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd" );
				
	public void start(){
		System.out.println("start archiv edge-records");
								
		//read out days to calculate
		List<Date> list_daysEdgeRecords = importDaysEdgeRecords();
						
		//archive edge records
		archivEdgeRecords(list_daysEdgeRecords);
		
		System.out.println("finished archiv edge-records");
		
	}
	
	private void archivEdgeRecords(List<Date> list_daysEdgeRecords) {
		//determine days to archive
		Iterator<Date> it_date = list_daysEdgeRecords.iterator();
		while(it_date.hasNext()){
			Date date = it_date.next();
			System.out.println("archive day: "+sdf.format(date));
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			//TODO: use holiday from each federal state
			//int daygroup = Function.determineDailyGroup(cal);
				
			int daygroup = Function.determineDailyGroupWithoutHolidays(cal);
			getTraveltime(date, daygroup);
		}
	}

		
	private void getTraveltime(Date date, int daygroup) {
		//System.out.println("import traveltime");
		
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			String query = "select sid, reverse, next_sid, entertime as entertime, exittime as exittime, ((exittime-entertime)/1000)+1 as tt from "+Config.table_raw_edge_records+" where DATE(to_timestamp(entertime/1000)) = '"+sdf.format(date)+"' and entertime > 0 and exittime >= entertime and sid != '' and next_sid != '' order by sid, next_sid";
			rs = st.executeQuery(query);
			//System.out.println("update traveltime-archiv");
			String sidOld = "";
			boolean reverseOld = false;
			String next_sidOld = "";
			HashMap<Long, Integer> hm_tt = new HashMap<>();
			while (rs.next()) {
					String sid = rs.getString("sid");
					boolean reverse = rs.getBoolean("reverse");
					String next_sid = rs.getString("next_sid");
					long entertime = rs.getLong("entertime");
					int tt = rs.getInt("tt");
					
					if(sidOld.isEmpty()){
						sidOld = sid;
						reverseOld = reverse;
						next_sidOld = next_sid;
					}
					if(sidOld.equals(sid) && next_sidOld.equals(next_sid)){
						hm_tt.put(entertime, tt);
					}
					else{
						//update archiv
						updateArchiv(sidOld, reverseOld, next_sidOld, hm_tt, daygroup, date);
						
						hm_tt.clear();
						sidOld = sid;
						reverseOld = reverse;
						next_sidOld = next_sid;
						hm_tt.put(entertime, tt);
					}
			}
			
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

	
	private List<Date> importDaysEdgeRecords() {
		List<Date> list_daysEdgeRecords = new LinkedList<>();
		Date today = new Date();
				
		String query = "select t1.date from "
				+ "(select DATE(to_timestamp(entertime/1000)) as date from "+Config.table_raw_edge_records+" where entertime > 0 and sid != '' and next_sid != '' group by date) as t1, "
				+ "(select t1.case as date from ("
					+ "SELECT CASE WHEN (SELECT COUNT(*) FROM "+Config.table_raw_edge_records_tt_archiv+") > 0 "
					+ "THEN (select DATE(to_timestamp(last_update/1000)) as date from "+Config.table_raw_edge_records_tt_archiv+" group by date order by date desc limit 1) " 
					+ "ELSE '2000-01-01' "
					+ "END) as t1) as t2 "
				+ "where t1.date > t2.date;";

		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				String date = rs.getString("date");
				//do not look at today
				if(!date.equals(sdf.format(today))){
					list_daysEdgeRecords.add(sdf.parse(date));
					System.out.println(date);
				}
				
			}
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
		return list_daysEdgeRecords;
	}
		
	private void updateArchiv(String sid, boolean reverse, String next_sid, HashMap<Long, Integer> hm_tt, int daygroup, Date date) {
		String delete= "DELETE FROM "+Config.table_raw_edge_records_tt_archiv+" WHERE sid='" + sid + "' AND next_sid='" + next_sid + "' AND daygroup=" + daygroup+";" ;
		String insert = "";
		//       hour             quartal         date   tt
		TreeMap<Integer, TreeMap<Integer, TreeMap<Date, List<Integer>>>> hm_intervall = new TreeMap<>();
		//System.out.println("update daily curve: "+sid+" "+next_sid);
		
		String query = "select * from "+Config.table_raw_edge_records_tt_archiv+" where sid = '"+sid+"' and next_sid = '"+next_sid+"' and daygroup = "+daygroup+"  ";
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			while (rs.next()) {
				//update existing daily curve
				String[] tt = rs.getString("traveltime").split(",");
				String[] tt_date = rs.getString("time").split(",");
				String[] quartal = rs.getString("from_quarter").replace("{", "").replace("}", "").split(",");
				
				for(int i=0; i<quartal.length; i++){
					int hour = Integer.valueOf((quartal[i].split(":"))[0]);
					int min = Integer.valueOf((quartal[i].split(":"))[1]);
					
					String[] da = tt_date[i].replace("{", "").replace("}", "").replace("\"", "").split(";");
					String[] t = tt[i].replace("{", "").replace("}", "").replace("\"", "").split(";");
					
					TreeMap<Date, List<Integer>> tm = new TreeMap<>(Collections.reverseOrder());
					for(int j = 0; j<t.length; j++){
						Date d = sdf.parse(da[j]);
						if(tm.containsKey(d)){
							tm.get(d).add(Integer.valueOf(t[j]));
						}
						else{
							List<Integer> list = new LinkedList<>();
							list.add(Integer.valueOf(t[j]));
							tm.put(d, list);
						}
					}
					
					if(hm_intervall.containsKey(hour)){
						hm_intervall.get(hour).put(min, tm);
					}
					else{
						TreeMap<Integer, TreeMap<Date, List<Integer>>> tm_q = new TreeMap<>();
						tm_q.put(min, tm);
						hm_intervall.put(hour, tm_q);
					}
					
					
				}
				
			}
			
			
			//System.out.println("neue Tagesganglinien erstellen "+sid+" "+next_sid);
			//neue Tagesganglinine erstellen
			String reisezeiten = "{";
			String zeiten = "{";
			String von_quartal = "{";
			String bis_quartal = "{";
				
			Iterator<Entry<Long, Integer>> it = hm_tt.entrySet().iterator();
			while(it.hasNext()){
				Entry<Long, Integer> entry = it.next();
				long zeit = entry.getKey();
				int tt = entry.getValue();
				getIntervall(zeit,tt, hm_intervall);
			}
				
			//maximale Anzahl an Reisezeiten pro Stundenquartal
			int maxtt_per_intervall = Config.maxtt_per_interval;
			
			//neue query generieren
			//Stunden
			Iterator<Entry<Integer, TreeMap<Integer, TreeMap<Date, List<Integer>>>>> it_intervall = hm_intervall.entrySet().iterator();
			while(it_intervall.hasNext()){
				Entry<Integer, TreeMap<Integer, TreeMap<Date, List<Integer>>>> entry = it_intervall.next();
				int hour = entry.getKey();
				//Minutenquartale
				TreeMap<Integer, TreeMap<Date, List<Integer>>> hm_quartal = entry.getValue();
				Iterator<Entry<Integer, TreeMap<Date, List<Integer>>>> it_quartal = hm_quartal.entrySet().iterator();
				while(it_quartal.hasNext()){
					Entry<Integer, TreeMap<Date, List<Integer>>> entry_quartal = it_quartal.next();
					int minute = entry_quartal.getKey();
					int anz_tt_values = 0;
						
					//Reisezeiten (Tag, Liste<Reisezeiten>)
					TreeMap<Date, List<Integer>> tm_date = entry_quartal.getValue();
					Iterator<Entry<Date, List<Integer>>> it_tm_date = tm_date.entrySet().iterator();
					String tt_date = "\"{";
					String tt = "\"{";
					
					while(it_tm_date.hasNext() && anz_tt_values <= maxtt_per_intervall){
						Entry<Date, List<Integer>> entry_tm_date = it_tm_date.next();
						Date date_tt = entry_tm_date.getKey();
						List<Integer> list_tt = entry_tm_date.getValue();
						Iterator<Integer> it_tt = list_tt.iterator();
						while(it_tt.hasNext() && anz_tt_values <= maxtt_per_intervall){
							tt += it_tt.next()+"";
							tt_date += sdf.format(date_tt);
							anz_tt_values++;
							if((it_tt.hasNext() || it_tm_date.hasNext()) && anz_tt_values <= maxtt_per_intervall){
								tt += ";";
								tt_date += ";";
							}
						}
					}
					
					tt += "}\"";
					tt_date += "}\"";
					//Quartale
					reisezeiten += tt;
					zeiten += tt_date;
					von_quartal += hour+":"+minute+":00";
					bis_quartal += hour+":"+(minute+14)+":59";
					if(it_quartal.hasNext() || it_intervall.hasNext()){
						reisezeiten += ",";
						zeiten  += ",";
						von_quartal += ",";
						bis_quartal += ",";
					}
				}
			}
				
			reisezeiten += "}";
			zeiten += "}";
			von_quartal += "}";
			bis_quartal += "}";
							
			insert=	"INSERT INTO "+Config.table_raw_edge_records_tt_archiv+" (sid, reverse, next_sid, daygroup, last_update, last_update_date, traveltime, time, from_quarter, until_quarter) " + 
					"VALUES ('"+ sid + "', '"+ reverse + "', '"+ next_sid + "', " + daygroup + ", '"+date.getTime()+"' , '"+sdf.format(date)+"','" +
					reisezeiten  + "', '" + zeiten + "', '" + von_quartal + "', '"+ bis_quartal + "')";
				
			//System.out.println(insert);
			st.execute(delete);
			st.execute(insert);
			
			
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
	
	private void getIntervall(long zeit, int tt, TreeMap<Integer, TreeMap<Integer, TreeMap<Date, List<Integer>>>> hm_intervall) {
		try{
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(zeit);
			//System.out.println(sdf.format(cal.getTime()));
			
			int minute = cal.get(Calendar.MINUTE);
			int hour = cal.get(Calendar.HOUR_OF_DAY);
			Date date = sdf.parse(sdf.format(cal.getTime()));
						
			int factor = minute / 15;
			int quartal = factor * 15;
			
			if(hm_intervall.containsKey(hour)){
				if(hm_intervall.get(hour).containsKey(quartal)){
					if(hm_intervall.get(hour).get(quartal).containsKey(date)){
						hm_intervall.get(hour).get(quartal).get(date).add(tt);
					}
					else{
						List<Integer> list = new LinkedList<>();
						list.add(tt);
						hm_intervall.get(hour).get(quartal).put(date, list);
					}
				}
				else{
					List<Integer> list = new LinkedList<>();
					list.add(tt);
					TreeMap<Date, List<Integer>> tm = new TreeMap<>(Collections.reverseOrder());
					tm.put(date, list);
					hm_intervall.get(hour).put(quartal, tm);
				}
			}
			else{
				List<Integer> list = new LinkedList<>();
				list.add(tt);
				TreeMap<Date, List<Integer>> tm = new TreeMap<>(Collections.reverseOrder());
				tm.put(date, list);
				TreeMap<Integer, TreeMap<Date, List<Integer>>> tm2 = new TreeMap<>();
				tm2.put(quartal, tm);
				hm_intervall.put(hour, tm2);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
