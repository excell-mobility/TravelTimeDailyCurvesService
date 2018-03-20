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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.json.JSONObject;

import excell.dailycurves.Function;
import excell.dailycurves.config.Config;

public class ArchivingSensorData {

private SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd" );
	
	public void start(){
		//read out days to calculate
		List<Date> list_daysSensorData = importDaysSensorData();
						
		//archiv speed-value from sensor-data
		archivSpeedvalue(list_daysSensorData);
		
	}
	
	private void archivSpeedvalue(List<Date> list_daysSensorData) {
		//zu archivierende Tage bestimmen
		Iterator<Date> it_date = list_daysSensorData.iterator();
		while(it_date.hasNext()){
			Date date = it_date.next();
			System.out.println("archive day: "+sdf.format(date));
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			//TODO: use holiday from each federal state
			//int daygroup = Function.determineDailyGroup(cal);
			
			int daygroup = Function.determineDailyGroupWithoutHolidays(cal);
			getSpeedValue(date, daygroup);
		}
	}

	private List<Date> importDaysSensorData() {
		List<Date> list_daysSensorData = new LinkedList<>();
		Date today = new Date();
		String query = "select t1.date from "
				+ "(select DATE(to_timestamp(time/1000)) as date from "+Config.table_raw_sensor_data+" group by date) as t1," 
				+ "(select t1.case as date from ("
				+ "SELECT CASE WHEN (SELECT COUNT(*) FROM "+Config.table_raw_sensor_data_speed_archiv+") > 0 "
					+ "THEN (select DATE(to_timestamp(last_update/1000)) as date from "+Config.table_raw_sensor_data_speed_archiv+" group by date order by date desc limit 1) " 
					+ "ELSE '2000-01-01' "
					+ "END) as t1) as t2 "
				+ "where t1.date > t2.date order by t1.date;";
		
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
					list_daysSensorData.add(sdf.parse(date));
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
		return list_daysSensorData;
	}
	
		
	private void getSpeedValue(Date date, int tagesgruppenID) {
		System.out.println("Geschwindigkeiten importieren");
		
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			String query = "select sensor_id, time as time, values from "+Config.table_raw_sensor_data+" where DATE(to_timestamp(time/1000)) = '"+sdf.format(date)+"' order by sensor_id, time";
			rs = st.executeQuery(query);
			System.out.println("Geschwindigkeitenarchiv updaten");
			String sidOld = "";
			TreeMap<Long, Integer> hm_speedvalues = new TreeMap<>();
			while (rs.next()) {
					String sid = rs.getString("sensor_id");
					String jsonValues = rs.getString("values");
					long time = rs.getLong("time");
					int speedValue = getJsonSpeedValue(jsonValues);
										
					if(sidOld.isEmpty()){
						sidOld = sid;
					}
					if(sidOld.equals(sid)){
						if(speedValue != -1)
							hm_speedvalues.put(time, speedValue);
					}
					else{
						//Tagesganglinie updaten
						if(!hm_speedvalues.isEmpty())
							updateArchiv(sidOld, hm_speedvalues, tagesgruppenID, date);
						
						hm_speedvalues.clear();
						sidOld = sid;
						if(speedValue != -1)
							hm_speedvalues.put(time, speedValue);
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
	
	private void updateArchiv(String sid, TreeMap<Long, Integer> hm_speedvalues, int tagesgruppenID, Date date) {
		//tagesgruppenID = 12;
		
		String delete= "DELETE FROM "+Config.table_raw_sensor_data_speed_archiv+" WHERE sensor_id='" + sid + "' AND daygroup = " + tagesgruppenID+";" ;
		String insert = "";
		//       hour             quartal         date   tt
		TreeMap<Integer, TreeMap<Integer, TreeMap<Date, Integer>>> hm_intervall = new TreeMap<>();
		//System.out.println("Tagesganglinie updaten: "+sid+" "+next_sid);
		
		String query = "select * from "+Config.table_raw_sensor_data_speed_archiv+" where sensor_id = '"+sid+"' and daygroup = "+tagesgruppenID+"  ";
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			while (rs.next()) {
				//System.out.println("Tagesganglinien vorhanden "+sid+" "+next_sid);
				//vorhandene Tagesganglinie updaten
							
				String[] speed = rs.getString("speed").split(",");
				String[] speed_date = rs.getString("times").split(",");
				String[] quarter = rs.getString("from_quarter").replace("{", "").replace("}", "").split(",");
				
				for(int i=0; i<quarter.length; i++){
					int hour = Integer.valueOf((quarter[i].split(":"))[0]);
					int min = Integer.valueOf((quarter[i].split(":"))[1]);
					
					String[] da = speed_date[i].replace("{", "").replace("}", "").replace("\"", "").split(";");
					String[] s = speed[i].replace("{", "").replace("}", "").replace("\"", "").split(";");
					
					TreeMap<Date, Integer> tm = new TreeMap<>(Collections.reverseOrder());
					for(int j = 0; j<s.length; j++){
						Date d = sdf.parse(da[j]);
						tm.put(d, Integer.valueOf(s[j]));
					}
					
					if(hm_intervall.containsKey(hour)){
						hm_intervall.get(hour).put(min, tm);
					}
					else{
						TreeMap<Integer, TreeMap<Date, Integer>> tm_q = new TreeMap<>();
						tm_q.put(min, tm);
						hm_intervall.put(hour, tm_q);
					}
				}
			}
			
			//System.out.println("neue Tagesganglinien erstellen "+sid+" "+next_sid);
			//neue Tagesganglinine erstellen
			String speedValues = "{";
			String times = "{";
			String from_quarter = "{";
			String until_quarter = "{";
				
			getIntervall(hm_speedvalues, hm_intervall);
			
				
			//maximale Anzahl an Reisezeiten pro Stundenquartal
			int maxSensorData_per_intervall = Config.maxSensorData_per_interval;
			
			//neue query generieren
			//Stunden
			Iterator<Entry<Integer, TreeMap<Integer, TreeMap<Date, Integer>>>> it_intervall = hm_intervall.entrySet().iterator();
			while(it_intervall.hasNext()){
				Entry<Integer, TreeMap<Integer, TreeMap<Date, Integer>>> entry = it_intervall.next();
				int hour = entry.getKey();
				//Minutenquartale
				TreeMap<Integer, TreeMap<Date, Integer>> hm_quartal = entry.getValue();
				Iterator<Entry<Integer, TreeMap<Date, Integer>>> it_quartal = hm_quartal.entrySet().iterator();
				while(it_quartal.hasNext()){
					Entry<Integer, TreeMap<Date, Integer>> entry_quartal = it_quartal.next();
					int minute = entry_quartal.getKey();
					int counter = 0;
						
					//Geschwindigkeiten (Tag, Geschwindigkeit)
					TreeMap<Date, Integer> tm_date = entry_quartal.getValue();
					Iterator<Entry<Date, Integer>> it_tm_date = tm_date.entrySet().iterator();
					String speedDateString = "\"{";
					String speedString = "\"{";
					
					while(it_tm_date.hasNext() && counter <= maxSensorData_per_intervall){
						Entry<Date, Integer> entry_tm_date = it_tm_date.next();
						Date date_speedValue = entry_tm_date.getKey();
						int speedValue = entry_tm_date.getValue();
						speedString += speedValue+"";
						speedDateString += sdf.format(date_speedValue);
						counter++;
						if((it_tm_date.hasNext()) && counter <= maxSensorData_per_intervall){
								speedString += ";";
								speedDateString += ";";
						}
					}
					
					
					speedString += "}\"";
					speedDateString += "}\"";
					//Quartale
					speedValues += speedString;
					times += speedDateString;
					from_quarter += hour+":"+minute+":00";
					until_quarter += hour+":"+(minute+14)+":59";
					if(it_quartal.hasNext() || it_intervall.hasNext()){
						speedValues += ",";
						times  += ",";
						from_quarter += ",";
						until_quarter += ",";
					}
				}
			}
				
			speedValues += "}";
			times += "}";
			from_quarter += "}";
			until_quarter += "}";
							
			insert=	"INSERT INTO "+Config.table_raw_sensor_data_speed_archiv+" (sensor_id, daygroup, last_update, last_update_date, speed, times, from_quarter, until_quarter) " + 
					"VALUES ('"+ sid + "', " + tagesgruppenID + ", '"+date.getTime()+"' , '"+sdf.format(date)+"','" +
					speedValues  + "', '" + times + "', '" + from_quarter + "', '"+ until_quarter + "')";
				
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
	
	private void getIntervall(TreeMap<Long, Integer> hm_speedvalues, TreeMap<Integer, TreeMap<Integer, TreeMap<Date, Integer>>> hm_intervall) {
		//long zeit, int speed, TreeMap<Integer, TreeMap<Integer, TreeMap<Date, List<Integer>>>> hm_intervall
		
		try{
			//Werte für ein Quartal zusammenfassen
			int hourOld = -1;
			int quarterOld = -1;
			int speedValue = 0;
			int counter = 0;
			
			Iterator<Entry<Long, Integer>> it = hm_speedvalues.entrySet().iterator();
			while(it.hasNext()){
				Entry<Long, Integer> entry = it.next();
				long zeit = entry.getKey();
				int speed = entry.getValue();
				
				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis(zeit);
				//System.out.println(sdf.format(cal.getTime()));
				
				int minute = cal.get(Calendar.MINUTE);
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				Date date = sdf.parse(sdf.format(cal.getTime()));
							
				int factor = minute / 15;
				int quarter = factor * 15;
				
				if(hourOld == -1) {
					hourOld = hour;
					quarterOld = quarter;
				}
				
				if(hourOld == hour && quarterOld == quarter) {
					speedValue += speed;
					counter ++;
					//Falls letzter Wert
					if(!it.hasNext()) {
						//calculate average
						speedValue = (int)((double)speedValue/counter);
						
						if(hm_intervall.containsKey(hourOld)){
							if(hm_intervall.get(hourOld).containsKey(quarterOld)){
								hm_intervall.get(hourOld).get(quarterOld).put(date, speedValue);
							}
							else{
								TreeMap<Date, Integer> tm = new TreeMap<>(Collections.reverseOrder());
								tm.put(date, speedValue);
								hm_intervall.get(hourOld).put(quarterOld, tm);
							}
						}
						else{
							
							TreeMap<Date, Integer> tm = new TreeMap<>(Collections.reverseOrder());
							tm.put(date, speedValue);
							TreeMap<Integer, TreeMap<Date, Integer>> tm2 = new TreeMap<>();
							tm2.put(quarterOld, tm);
							hm_intervall.put(hourOld, tm2);
						}
					}
					
				}
				else {
					//calculate average
					speedValue = (int)((double)speedValue/counter);
					
					if(hm_intervall.containsKey(hourOld)){
						if(hm_intervall.get(hourOld).containsKey(quarterOld)){
							hm_intervall.get(hourOld).get(quarterOld).put(date, speedValue);
						}
						else{
							TreeMap<Date, Integer> tm = new TreeMap<>(Collections.reverseOrder());
							tm.put(date, speedValue);
							hm_intervall.get(hourOld).put(quarterOld, tm);
						}
					}
					else{
						
						TreeMap<Date, Integer> tm = new TreeMap<>(Collections.reverseOrder());
						tm.put(date, speedValue);
						TreeMap<Integer, TreeMap<Date, Integer>> tm2 = new TreeMap<>();
						tm2.put(quarterOld, tm);
						hm_intervall.put(hourOld, tm2);
					}
					speedValue = 0;
					counter = 0;
					hourOld = hour;
					quarterOld = quarter;
					
					speedValue += speed;
					counter ++;
				}
			}
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}

	
	private int getJsonSpeedValue(String jsonString) {
		int v = -1;
		JSONObject jsonObject = new JSONObject(jsonString);
		try {
			if(jsonObject.getInt("Belegung") > 0)
				v = jsonObject.getInt("Geschwindigkeit");
		}
		catch(Exception e) {
			//e.printStackTrace();
		}
		return v;
	}	
}
