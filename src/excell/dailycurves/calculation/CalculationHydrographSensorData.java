package excell.dailycurves.calculation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import excell.dailycurves.config.Config;

public class CalculationHydrographSensorData {
	
	private SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
	private SimpleDateFormat sdf_date = new SimpleDateFormat( "yyyy-MM-dd" );
	private SimpleDateFormat sdf_quartal = new SimpleDateFormat( "HH:mm:ss" );
	
	public void start() {
		Date currentDate = new Date();
		//1. determine last updates from table sensor_data_speed_archiv
		HashMap<Integer, Date> hm_lastUpdates = getLastUpdateSensorDataArchiv();
		
		//2. determine last updates from table sensor_data_speed_hydrograph
		HashMap<Integer, Date> hm_lastUpdatesHydrograph = getLastUpdate_sensor_data_speed_hydrograph();
		
		//calculate hydrograph
		Iterator<Entry<Integer, Date>> it_lastUpdate = hm_lastUpdates.entrySet().iterator();
		while(it_lastUpdate.hasNext()){
			boolean calculate = true;
			Entry<Integer, Date> entry_lastUpdate = it_lastUpdate.next();
			int daygroup = entry_lastUpdate.getKey();
			Date dg_date = entry_lastUpdate.getValue();
			
			//check if day needs to be recalculated
			if(hm_lastUpdatesHydrograph.containsKey(daygroup)){
				if(dg_date.getTime() < hm_lastUpdatesHydrograph.get(daygroup).getTime())
					calculate = false;
			}
			
			//recalculate hydrograph
			if(calculate){
				calculateDg(daygroup, currentDate);
			}
		}
	}
	
	private HashMap<Integer, Date> getLastUpdateSensorDataArchiv() {
		HashMap<Integer, Date> hm_lastUpdates = new HashMap<>();
		//System.out.println("latest updates from sensor_data_speed_archiv");
		String query = "select t1.daygroup, (select last_update_date from "+Config.table_raw_sensor_data_speed_archiv+" where daygroup = t1.daygroup order by last_update_date desc limit 1) from "
				+ "(select daygroup from "+Config.table_raw_sensor_data_speed_archiv+" group by daygroup) as t1 order by daygroup, last_update_date;";
		
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				hm_lastUpdates.put(rs.getInt("daygroup"), sdf_date.parse(rs.getString("last_update_date")));
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
		return hm_lastUpdates;
	}
	
	private HashMap<Integer, Date> getLastUpdate_sensor_data_speed_hydrograph() {
		HashMap<Integer, Date> hm_lastUpdatesHydrograph = new HashMap<>();
		//System.out.println("get latest updates from table sensor_data_speed_hydrograph");
		String query = "select t1.daygroup, (select last_update_date from "+Config.table_proc_sensor_data_speed_hydrograph+" where daygroup = t1.daygroup order by last_update_date desc limit 1) from "
				+ "(select daygroup from "+Config.table_proc_sensor_data_speed_hydrograph+" group by daygroup) as t1 order by daygroup, last_update_date";
		
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				hm_lastUpdatesHydrograph.put(rs.getInt("daygroup"), sdf_date.parse(rs.getString("last_update_date")));
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
		return hm_lastUpdatesHydrograph;
		
	}
	
	private void calculateDg(int daygroup, Date currentDate) {
		System.out.println("hydrograph of daygroup "+daygroup+" are recalculate");
		
		String query = "select * from "+Config.table_raw_sensor_data_speed_archiv+" where daygroup = "+daygroup;
		Statement st = null;
		ResultSet rs = null;
		Connection conn = null;
		Connection conn2 = null;
		Statement st2 = null;
		
		try{
			conn = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			conn2 = DriverManager.getConnection("jdbc:postgresql://" + Config.excelldb_host + ":" + Config.excelldb_port + "/" + Config.excelldb_name + "", Config.excelldb_user, Config.excelldb_pass);
			st = conn.createStatement();
			st2 = conn2.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				int sensor_id = rs.getInt("sensor_id");
								
				String[] speedValues = rs.getString("speed").replace("{", "").replace("}", "").replace("\"", "").split(",");
				String[] speedDate = rs.getString("times").replace("{", "").replace("}", "").replace("\"", "").split(",");
				String[] from_quarter = rs.getString("from_quarter").replace("{", "").replace("}", "").split(",");
				String[] until_quarter = rs.getString("until_quarter").replace("{", "").replace("}", "").split(",");
				
				String delete = "Delete from "+Config.table_proc_sensor_data_speed_hydrograph+" where sensor_id = '"+sensor_id+"' and daygroup = "+daygroup+" ;";
																			
				//calculate average (exponential smoothing) and variance for each quarter individually + interpolation + smoothing
				String sqlInsert = calculateHydrograph(sensor_id, daygroup,speedValues, speedDate, from_quarter, until_quarter, currentDate);
				
				//In DB schreiben
				st2.execute(delete);
				st2.execute(sqlInsert);
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
			try{
				if (st2 != null)	
					st2.close();
				st2 = null;} 
			catch (Exception ex) {/* nothing to do */}
			try {
				if (conn != null) 
					conn.close();
				conn = null;} 
			catch (Exception ex) {/* nothing to do */}
			try {
				if (conn2 != null) 
					conn2.close();
				conn2 = null;} 
			catch (Exception ex) {/* nothing to do */}
		}
	}

	private String calculateHydrograph(int sensor_id, int daygroup, String[] speedValues, String[] speedDate,
		String[] from_quarter, String[] until_quarter, Date currentDate) {
		//System.out.println("*******************************************************");
		//exponential smoothing
		int[] speed_averageArray = new int[from_quarter.length];
		int[] speed_varianceArray = new int[from_quarter.length];
		int[] speed_numberOfValues = new int[from_quarter.length];
		for(int i=0; i<from_quarter.length; i++){
			//speed-values
			String[] speed_string = speedValues[i].split(";");
			double speed_average = Integer.valueOf(speed_string[0]);
			//exponential smoothing
			for(int t=1; t<speed_string.length; t++){
				speed_average = Config.alpha*speed_average + (1-Config.alpha)*Integer.valueOf(speed_string[t]);
				//System.out.println(Integer.valueOf(speed_string[t]));
			}
			//System.out.println("speed average: "+speed_average);
					
			//variance
			double var = 0;
			for(int t=0; t<speed_string.length; t++){
				var += (Integer.valueOf(speed_string[t])-speed_average)*(Integer.valueOf(speed_string[t])-speed_average);
			}		
			var = var/(double)speed_string.length;
			//System.out.println("variance: "+var);
					
			//standard deviation
			var = Math.sqrt(var);
			
			speed_averageArray[i] = (int)Math.round(speed_average);
			speed_varianceArray[i] = (int) Math.round(var);
			speed_numberOfValues[i] += speed_string.length;
		}
				
		//interpolation
		LinkedList<Integer> list_speedAverage = new LinkedList<>();
		LinkedList<String> list_fromQuartal = new LinkedList<>();
		LinkedList<String> list_untilQuartal = new LinkedList<>();
		LinkedList<Integer> list_numberOfValues = new LinkedList<>();
		LinkedList<Integer> list_variance = new LinkedList<>();
		
		interpolation(from_quarter,until_quarter, speed_averageArray, speed_varianceArray,speed_numberOfValues, list_speedAverage, list_fromQuartal,list_untilQuartal, list_numberOfValues, list_variance);
		
		//SQL-Insert
		String insert = creatSqlStatement(sensor_id, daygroup, list_speedAverage, list_fromQuartal, list_untilQuartal, list_numberOfValues, list_variance, currentDate);
		
		return insert;
	}
	
	private void interpolation(String[] from_q, String[] until_q, int[] averageArray, int[] varianceArray, int[] numberOfValues, LinkedList<Integer> list_Average, LinkedList<String> list_fromQuartal, LinkedList<String> list_untilQuartal, LinkedList<Integer> list_numberOfValues, LinkedList<Integer> list_variance) {
		try{
			Calendar cal = Calendar.getInstance();
			Calendar untilCal = Calendar.getInstance();
			Calendar nextCal = Calendar.getInstance();
			Calendar untilNextCal = Calendar.getInstance();
			cal.setTime(sdf_quartal.parse(from_q[0]));
			untilCal.setTime(sdf_quartal.parse(until_q[0]));
			nextCal.setTime(sdf_quartal.parse(from_q[0]));
			nextCal.add(Calendar.MINUTE, 15);
			untilNextCal.setTime(sdf_quartal.parse(until_q[0]));
			untilNextCal.add(Calendar.MINUTE, 15);
			int tt_interpolationValueA = averageArray[0];
			int tt_interpolationValueB = averageArray[0];
			int var_interpolationValueA = varianceArray[0];
			int var_interpolationValueB = varianceArray[0];
									
			//fill in first missing quarters
			Calendar calStart = Calendar.getInstance();
			Calendar untilCalStart = Calendar.getInstance();
			calStart.setTime(sdf_quartal.parse("00:00:00"));
			untilCalStart.setTime(sdf_quartal.parse("00:14:59"));
			long diff = cal.getTimeInMillis()-calStart.getTimeInMillis();
			diff = diff/1000/60;
			int numMissingValues = (int) (diff/15);
			if(numMissingValues > 0 && numMissingValues < 5){
				//insert missing values
				for(int j=0; j<numMissingValues; j++){
					list_Average.add(averageArray[0]);
					list_fromQuartal.add(sdf_quartal.format(calStart.getTime()));
					list_untilQuartal.add(sdf_quartal.format(untilCalStart.getTime()));
					list_numberOfValues.add(0);
					list_variance.add(varianceArray[0]);
					
					calStart.add(Calendar.MINUTE, 15);
					untilCalStart.add(Calendar.MINUTE, 15);
				}
			}
			
			//enter the first measured value
			list_Average.add(averageArray[0]);
			list_fromQuartal.add(sdf_quartal.format(cal.getTime()));
			list_untilQuartal.add(sdf_quartal.format(untilCal.getTime()));
			list_numberOfValues.add(numberOfValues[0]);
			list_variance.add(varianceArray[0]);
			
			//interpolation
			for(int i=1; i<from_q.length; i++){
				cal.setTime(sdf_quartal.parse(from_q[i]));
				untilCal.setTime(sdf_quartal.parse(until_q[i]));
				//the quarters are equal
				if(nextCal.compareTo(cal) == 0){
					tt_interpolationValueA = averageArray[i];
					var_interpolationValueA = varianceArray[i];
										
					list_Average.add(averageArray[i]);
					list_fromQuartal.add(sdf_quartal.format(cal.getTime()));
					list_untilQuartal.add(sdf_quartal.format(untilCal.getTime()));
					list_numberOfValues.add(numberOfValues[i]);
					list_variance.add(varianceArray[i]);
					
					nextCal.add(Calendar.MINUTE, 15);
					untilNextCal.add(Calendar.MINUTE, 15);
				}
				//the quarters are not equal
				else{
					diff = cal.getTimeInMillis()-nextCal.getTimeInMillis();
					diff = diff/1000/60;
					numMissingValues = (int) (diff/15);
					//if the number of missing values is justifiable, then interpolate
					if(numMissingValues <= Config.numMissingQuarter){
						tt_interpolationValueB = averageArray[i];
						var_interpolationValueB = varianceArray[i];
						//interpolation equation calculate slope value
						double m = ((double)tt_interpolationValueB - tt_interpolationValueA)/(numMissingValues+1);
						double mVar = ((double)var_interpolationValueB - var_interpolationValueA)/(numMissingValues+1);
																	
						//insert interpolated values
						for(int j=1; j<=numMissingValues; j++){
							int averageValue = (int)Math.round((m*j)+tt_interpolationValueA);  							
							int varValue = (int)Math.round((mVar*j)+var_interpolationValueA);
							
							list_Average.add(averageValue);
							list_fromQuartal.add(sdf_quartal.format(nextCal.getTime()));
							list_untilQuartal.add(sdf_quartal.format(untilNextCal.getTime()));
							list_numberOfValues.add(0);
							list_variance.add(varValue);
							
							nextCal.add(Calendar.MINUTE, 15);
							untilNextCal.add(Calendar.MINUTE, 15);
						}
						
						//insert current quarter
						list_Average.add(averageArray[i]);
						list_fromQuartal.add(sdf_quartal.format(cal.getTime()));
						list_untilQuartal.add(sdf_quartal.format(untilCal.getTime()));
						list_numberOfValues.add(numberOfValues[i]);
						list_variance.add(varianceArray[i]);
						
						nextCal.add(Calendar.MINUTE, 15);
						untilNextCal.add(Calendar.MINUTE, 15);
						
						tt_interpolationValueA = averageArray[i];
						var_interpolationValueA = varianceArray[i];
					}
				}
			}
			
			//fill up quarters
			Calendar calEnd = Calendar.getInstance();
			cal.setTime(sdf_quartal.parse(from_q[from_q.length-1]));
			untilCal.setTime(sdf_quartal.parse(until_q[until_q.length-1]));
			calEnd.setTime(sdf_quartal.parse("23:45:00"));
			diff = calEnd.getTimeInMillis()-cal.getTimeInMillis();
			diff = diff/1000/60;
			numMissingValues = (int) (diff/15);
			if(numMissingValues > 0 && numMissingValues < 5){
				//insert missing values
				for(int j=0; j<numMissingValues; j++){
					cal.add(Calendar.MINUTE, 15);
					untilCal.add(Calendar.MINUTE, 15);
					list_Average.add(averageArray[averageArray.length-1]);
					list_fromQuartal.add(sdf_quartal.format(cal.getTime()));
					list_untilQuartal.add(sdf_quartal.format(untilCal.getTime()));
					list_numberOfValues.add(0);
					list_variance.add(varianceArray[varianceArray.length-1]);
				}
			}
			
			
			//smoothing only with complete daily gait (no gaps)
			LinkedList<Integer> list_ttExponentialSmoothing = new LinkedList<>();
			if(list_fromQuartal.size() == 96){
				for(int i=0; i<list_Average.size(); i++){
					if(i>1 && i < list_Average.size()-2){
						double value = ((1.0/3)*list_Average.get(i-2))+((2.0/3)*list_Average.get(i-1))+list_Average.get(i)+((2.0/3)*list_Average.get(i+1))+((1.0/3)*list_Average.get(i+2));
						value = value / 3;
						list_ttExponentialSmoothing.add((int)Math.round(value));
					}
					else{
						list_ttExponentialSmoothing.add(list_Average.get(i));
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private String creatSqlStatement(int sensor_id, int daygroup, LinkedList<Integer> list_ttAverage, LinkedList<String> list_fromQuartal, LinkedList<String> list_untilQuartal, LinkedList<Integer> list_numberOfValues, LinkedList<Integer> list_variance, Date currentDate) {
		//create sql-inserts
		String speedValues = "{";
		String variance = "{";
		String from_quartal = "{";
		String until_quartal = "{";
		String numberOfValues = "{";
		
		int i=0;
		Iterator<String> it = list_fromQuartal.iterator();
		while(it.hasNext()){
			speedValues += Math.round(list_ttAverage.get(i));
			variance += Math.round(list_variance.get(i));
			from_quartal += it.next();
			until_quartal += list_untilQuartal.get(i);
			numberOfValues += list_numberOfValues.get(i);
										
			if(it.hasNext()){
				speedValues += ",";
				variance += ",";
				from_quartal += ",";
				until_quartal += ",";
				numberOfValues += ",";
			}
			i++;
		}
		speedValues += "}";
		variance += "}";
		from_quartal += "}";
		until_quartal += "}";
		numberOfValues += "}";
				
		//String insert = "Delete from "+Config.table_tt_hydrograph_edges+" where sid = '"+sid+"' and next_sid = '"+next_sid+"' and tg = "+daygroup+" ;";
				
		String insert =	"INSERT INTO "+Config.table_proc_sensor_data_speed_hydrograph+" (sensor_id, daygroup, last_update, last_update_date, speed, standard_deviation, from_quarter, until_quarter, number_of_values) " + 
				"VALUES ('"+ sensor_id + "', " + daygroup + ", '"+currentDate.getTime()+"' , '"+sdf.format(currentDate)+"','" +
				speedValues  + "', '" + variance + "', '" + from_quartal + "', '"+ until_quartal + "', '"+ numberOfValues + "');";
		return insert;
	}

}
