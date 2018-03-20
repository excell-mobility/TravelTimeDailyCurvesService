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

public class CalculationHydrographEdgeRecords{
	
	private SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
	private SimpleDateFormat sdf_date = new SimpleDateFormat( "yyyy-MM-dd" );
	private SimpleDateFormat sdf_quarter = new SimpleDateFormat( "HH:mm:ss" );
	
	public CalculationHydrographEdgeRecords () {
		
	}
	
	
	public void start() {
		Date currentDate = new Date();
		//1. determine last updates from table the edge_records_tt_archiv
		HashMap<Integer, Date> hm_lastUpdates = getLastUpdateEdgeRecordsTtArchiv();
		
		//2. determine last updates from table tt_hydrograph_edges
		HashMap<Integer, Date> hm_lastUpdatesHydrograph = getLastUpdate_tt_hydrograph_edges();
		
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
			
		
	private HashMap<Integer, Date> getLastUpdateEdgeRecordsTtArchiv() {
		HashMap<Integer, Date> hm_lastUpdates = new HashMap<>();
		//System.out.println("latest updates from table edge_records_tt_archiv are read");
		String query = "select t1.daygroup, (select last_update_date from "+Config.table_raw_edge_records_tt_archiv+" where daygroup = t1.daygroup order by last_update_date desc limit 1) from "
				+ "(select daygroup from "+Config.table_raw_edge_records_tt_archiv+" group by daygroup) as t1 order by daygroup, last_update_date";
		
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
	
	private HashMap<Integer, Date> getLastUpdate_tt_hydrograph_edges() {
		HashMap<Integer, Date> hm_lastUpdatesHydrograph = new HashMap<>();
		//System.out.println("get latest updates from table tt_hydrograph_edges");
		String query = "select t1.daygroup, (select last_update_date from "+Config.table_tt_hydrograph_edges+" where daygroup = t1.daygroup order by last_update_date desc limit 1) from "
				+ "(select daygroup from "+Config.table_tt_hydrograph_edges+" group by daygroup) as t1 order by daygroup, last_update_date";
		
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
		String query = "select * from "+Config.table_raw_edge_records_tt_archiv+" where daygroup = "+daygroup;
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
				try {
					String sid = rs.getString("sid");
					String next_sid = rs.getString("next_sid");
					boolean reverse = rs.getBoolean("reverse");
					
					String[] tt = rs.getString("traveltime").replace("{", "").replace("}", "").replace("\"", "").split(",");
					String[] tt_date = rs.getString("time").replace("{", "").replace("}", "").replace("\"", "").split(",");
					String[] quarter_from = rs.getString("from_quarter").replace("{", "").replace("}", "").split(",");
					String[] quarter_until = rs.getString("until_quarter").replace("{", "").replace("}", "").split(",");
					
					String delete = "Delete from "+Config.table_tt_hydrograph_edges+" where sid = '"+sid+"' and next_sid = '"+next_sid+"' and daygroup = "+daygroup+" ;";
									
					//1. variant: calculate average and variance for each quarter individually
					//String sqlInsert = calculateHydrographVar1(sid, next_sid, daygroup,tt, tt_date, quarter_from, quarter_until, currentDate);
													
					//2. variante: calculate average (exponential smoothing) and variance for each quarter individually + interpolation + smoothing
					String sqlInsert = calculateHydrographVar2(sid, reverse, next_sid, daygroup,tt, tt_date, quarter_from, quarter_until, currentDate);
					
					//export into database
					st2.execute(delete);
					st2.execute(sqlInsert);
				}
				catch(Exception e) {};
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
	
	/**
	 * Mittelwertbildung
	 * 
	 * @param sid
	 * @param next_sid
	 * @param daygroup
	 * @param tt
	 * @param tt_date
	 * @param from_q
	 * @param until_q
	 * @param currentDate
	 * @return
	 */
	private String calculateHydrographVar1(String sid, String next_sid, int daygroup, String[] tt, String[] tt_date, String[] from_q, String[] until_q, Date currentDate) {
		String traveltime = "{";
		String variance = "{";
		String from_quarter = "{";
		String until_quarter = "{";
		String numberOfValues = "{";
		
		//System.out.println("*******************************************************");
		for(int i=0; i<from_q.length; i++){
			//traveltime
			String[] tt_string = tt[i].split(";");
			double tt_average = Integer.valueOf(tt_string[0]);
			//exponential smoothing
			for(int t=1; t<tt_string.length; t++){
				tt_average = Config.alpha*tt_average + (1-Config.alpha)*Integer.valueOf(tt_string[t]);
				//System.out.println(Integer.valueOf(tt_string[t]));
			}
			//System.out.println("average: "+tt_average);
			
			//variance
			double var = 0;
			for(int t=0; t<tt_string.length; t++){
				var += (Integer.valueOf(tt_string[t])-tt_average)*(Integer.valueOf(tt_string[t])-tt_average);
			}		
			var = var/(double)tt_string.length;
			//System.out.println("variance: "+var);
			
			//standard deviation
			var = Math.sqrt(var);
			//System.out.println("standard deviation: "+var);
			//System.out.println("standard deviation: "+Math.round(var));
			
			traveltime += Math.round(tt_average);
			variance += Math.round(var);
			from_quarter += from_q[i];
			until_quarter += until_q[i];
			numberOfValues += tt_string.length;
			
			if((i+1) < from_q.length){
				traveltime += ",";
				variance += ",";
				from_quarter += ",";
				until_quarter += ",";
				numberOfValues += ",";
			}
			
		}
		
		traveltime += "}";
		variance += "}";
		from_quarter += "}";
		until_quarter += "}";
		numberOfValues += "}";
		
		//String insert = "Delete from "+Config.table_tt_hydrograph_edges+" where sid = '"+sid+"' and next_sid = '"+next_sid+"' and tg = "+daygroup+" ;";
		
		String insert =	"INSERT INTO "+Config.table_tt_hydrograph_edges+" (sid, next_sid, tg, last_update, last_update_date, traveltime, standard_deviation, from_quarter, until_quarter, number_of_values) " + 
				"VALUES ('"+ sid + "', '"+ next_sid + "', " + daygroup + ", '"+currentDate.getTime()+"' , '"+sdf.format(currentDate)+"','" +
				traveltime  + "', '" + variance + "', '" + from_quarter + "', '"+ until_quarter + "', '"+ numberOfValues + "');";
		
		//System.out.println(insert);
		return insert;
	}
	
	/**
	 * calculate average (exponential smoothing) and variance for each quarter individually + interpolation + smoothing
	 * 
	 * @param sid
	 * @param reverse 
	 * @param next_sid
	 * @param daygroup
	 * @param tt
	 * @param tt_date
	 * @param from_q
	 * @param until_q
	 * @param currentDate
	 * @return
	 */
	private String calculateHydrographVar2(String sid, boolean reverse, String next_sid, int daygroup, String[] tt, String[] tt_date, String[] from_q, String[] until_q, Date currentDate) {
		//System.out.println("*******************************************************");
		//exponential smoothing
		int[] tt_averageArray = new int[from_q.length];
		int[] tt_varianceArray = new int[from_q.length];
		int[] tt_numberOfValues = new int[from_q.length];
		for(int i=0; i<from_q.length; i++){
			//traveltime
			String[] tt_string = tt[i].split(";");
			double tt_average = Integer.valueOf(tt_string[0]);
			//exponential smoothing
			for(int t=1; t<tt_string.length; t++){
				tt_average = Config.alpha*tt_average + (1-Config.alpha)*Integer.valueOf(tt_string[t]);
				//System.out.println(Integer.valueOf(tt_string[t]));
			}
			//System.out.println("average: "+tt_average);
			
			//variance
			double var = 0;
			for(int t=0; t<tt_string.length; t++){
				var += (Integer.valueOf(tt_string[t])-tt_average)*(Integer.valueOf(tt_string[t])-tt_average);
			}		
			var = var/(double)tt_string.length;
			//System.out.println("variance: "+var);
			
			//standard deviation 
			var = Math.sqrt(var);
						
			tt_averageArray[i] = (int)Math.round(tt_average);
			tt_varianceArray[i] = (int) Math.round(var);
			tt_numberOfValues[i] += tt_string.length;
		}
		
		//interpolation
		LinkedList<Integer> list_ttAverage = new LinkedList<>();
		LinkedList<String> list_fromQuarter = new LinkedList<>();
		LinkedList<String> list_untilQuarter = new LinkedList<>();
		LinkedList<Integer> list_numberOfValues = new LinkedList<>();
		LinkedList<Integer> list_variance = new LinkedList<>();
		
		interpolation(from_q,until_q, tt_averageArray, tt_varianceArray,tt_numberOfValues, list_ttAverage, list_fromQuarter,list_untilQuarter, list_numberOfValues, list_variance);
		
		//SQL-Insert
		String insert = creatSqlStatement(sid, reverse, next_sid, daygroup, list_ttAverage, list_fromQuarter, list_untilQuarter, list_numberOfValues, list_variance, currentDate);
		
		return insert;
	}


	private String creatSqlStatement(String sid, boolean reverse, String next_sid, int daygroup, LinkedList<Integer> list_ttAverage, LinkedList<String> list_fromQuarter, LinkedList<String> list_untilQuarter, LinkedList<Integer> list_numberOfValues, LinkedList<Integer> list_variance, Date currentDate) {
		//create sql-inserts
		String traveltime = "{";
		String variance = "{";
		String from_quarter = "{";
		String until_quarter = "{";
		String numberOfValues = "{";
		
		int i=0;
		Iterator<String> it = list_fromQuarter.iterator();
		while(it.hasNext()){
			traveltime += Math.round(list_ttAverage.get(i));
			variance += Math.round(list_variance.get(i));
			from_quarter += it.next();
			until_quarter += list_untilQuarter.get(i);
			numberOfValues += list_numberOfValues.get(i);
										
			if(it.hasNext()){
				traveltime += ",";
				variance += ",";
				from_quarter += ",";
				until_quarter += ",";
				numberOfValues += ",";
			}
			i++;
		}
		traveltime += "}";
		variance += "}";
		from_quarter += "}";
		until_quarter += "}";
		numberOfValues += "}";
				
		//String insert = "Delete from "+Config.table_tt_hydrograph_edges+" where sid = '"+sid+"' and next_sid = '"+next_sid+"' and tg = "+daygroup+" ;";
				
		String insert =	"INSERT INTO "+Config.table_tt_hydrograph_edges+" (sid, reverse, next_sid, daygroup, last_update, last_update_date, traveltime, standard_deviation, from_quarter, until_quarter, number_of_values) " + 
				"VALUES ('"+ sid + "','"+ reverse + "', '"+ next_sid + "', " + daygroup + ", '"+currentDate.getTime()+"' , '"+sdf.format(currentDate)+"','" +
				traveltime  + "', '" + variance + "', '" + from_quarter + "', '"+ until_quarter + "', '"+ numberOfValues + "');";
		return insert;
	}


	private void interpolation(String[] from_q, String[] until_q, int[] tt_averageArray, int[] varianceArray, int[] tt_numberOfValues, LinkedList<Integer> list_ttAverage, LinkedList<String> list_fromQuarter, LinkedList<String> list_untilQuarter, LinkedList<Integer> list_numberOfValues, LinkedList<Integer> list_variance) {
		try{
			Calendar cal = Calendar.getInstance();
			Calendar untilCal = Calendar.getInstance();
			Calendar nextCal = Calendar.getInstance();
			Calendar untilNextCal = Calendar.getInstance();
			cal.setTime(sdf_quarter.parse(from_q[0]));
			untilCal.setTime(sdf_quarter.parse(until_q[0]));
			nextCal.setTime(sdf_quarter.parse(from_q[0]));
			nextCal.add(Calendar.MINUTE, 15);
			untilNextCal.setTime(sdf_quarter.parse(until_q[0]));
			untilNextCal.add(Calendar.MINUTE, 15);
			int tt_interpolationValueA = tt_averageArray[0];
			int tt_interpolationValueB = tt_averageArray[0];
			int var_interpolationValueA = varianceArray[0];
			int var_interpolationValueB = varianceArray[0];
									
			//fill in first missing quarters
			Calendar calStart = Calendar.getInstance();
			Calendar untilCalStart = Calendar.getInstance();
			calStart.setTime(sdf_quarter.parse("00:00:00"));
			untilCalStart.setTime(sdf_quarter.parse("00:14:59"));
			long diff = cal.getTimeInMillis()-calStart.getTimeInMillis();
			diff = diff/1000/60;
			int numMissingValues = (int) (diff/15);
			if(numMissingValues > 0 && numMissingValues < 5){
				//insert missing values
				for(int j=0; j<numMissingValues; j++){
					list_ttAverage.add(tt_averageArray[0]);
					list_fromQuarter.add(sdf_quarter.format(calStart.getTime()));
					list_untilQuarter.add(sdf_quarter.format(untilCalStart.getTime()));
					list_numberOfValues.add(0);
					list_variance.add(varianceArray[0]);
					
					calStart.add(Calendar.MINUTE, 15);
					untilCalStart.add(Calendar.MINUTE, 15);
				}
			}
			
			//enter the first measured value
			list_ttAverage.add(tt_averageArray[0]);
			list_fromQuarter.add(sdf_quarter.format(cal.getTime()));
			list_untilQuarter.add(sdf_quarter.format(untilCal.getTime()));
			list_numberOfValues.add(tt_numberOfValues[0]);
			list_variance.add(varianceArray[0]);
			
			//interpolation
			for(int i=1; i<from_q.length; i++){
				cal.setTime(sdf_quarter.parse(from_q[i]));
				untilCal.setTime(sdf_quarter.parse(until_q[i]));
				//the quarters are equal
				if(nextCal.compareTo(cal) == 0){
					tt_interpolationValueA = tt_averageArray[i];
					var_interpolationValueA = varianceArray[i];
										
					list_ttAverage.add(tt_averageArray[i]);
					list_fromQuarter.add(sdf_quarter.format(cal.getTime()));
					list_untilQuarter.add(sdf_quarter.format(untilCal.getTime()));
					list_numberOfValues.add(tt_numberOfValues[i]);
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
						tt_interpolationValueB = tt_averageArray[i];
						var_interpolationValueB = varianceArray[i];
						//interpolation equation calculate slope value
						double m = ((double)tt_interpolationValueB - tt_interpolationValueA)/(numMissingValues+1);
						double mVar = ((double)var_interpolationValueB - var_interpolationValueA)/(numMissingValues+1);
																	
						//insert interpolated values
						for(int j=1; j<=numMissingValues; j++){
							int averageValue = (int)Math.round((m*j)+tt_interpolationValueA);  							
							int varValue = (int)Math.round((mVar*j)+var_interpolationValueA);
							
							list_ttAverage.add(averageValue);
							list_fromQuarter.add(sdf_quarter.format(nextCal.getTime()));
							list_untilQuarter.add(sdf_quarter.format(untilNextCal.getTime()));
							list_numberOfValues.add(0);
							list_variance.add(varValue);
							
							nextCal.add(Calendar.MINUTE, 15);
							untilNextCal.add(Calendar.MINUTE, 15);
						}
						
						//insert current quarter
						list_ttAverage.add(tt_averageArray[i]);
						list_fromQuarter.add(sdf_quarter.format(cal.getTime()));
						list_untilQuarter.add(sdf_quarter.format(untilCal.getTime()));
						list_numberOfValues.add(tt_numberOfValues[i]);
						list_variance.add(varianceArray[i]);
						
						nextCal.add(Calendar.MINUTE, 15);
						untilNextCal.add(Calendar.MINUTE, 15);
						
						tt_interpolationValueA = tt_averageArray[i];
						var_interpolationValueA = varianceArray[i];
					}
				}
			}
			
			//fill up quarters
			Calendar calEnd = Calendar.getInstance();
			cal.setTime(sdf_quarter.parse(from_q[from_q.length-1]));
			untilCal.setTime(sdf_quarter.parse(until_q[until_q.length-1]));
			calEnd.setTime(sdf_quarter.parse("23:45:00"));
			diff = calEnd.getTimeInMillis()-cal.getTimeInMillis();
			diff = diff/1000/60;
			numMissingValues = (int) (diff/15);
			if(numMissingValues > 0 && numMissingValues < 5){
				//insert missing values
				for(int j=0; j<numMissingValues; j++){
					cal.add(Calendar.MINUTE, 15);
					untilCal.add(Calendar.MINUTE, 15);
					list_ttAverage.add(tt_averageArray[tt_averageArray.length-1]);
					list_fromQuarter.add(sdf_quarter.format(cal.getTime()));
					list_untilQuarter.add(sdf_quarter.format(untilCal.getTime()));
					list_numberOfValues.add(0);
					list_variance.add(varianceArray[varianceArray.length-1]);
				}
			}
			
			
			//smoothing only with complete daily gait (no gaps)
			LinkedList<Integer> list_ttExponentialSmoothing = new LinkedList<>();
			if(list_fromQuarter.size() == 96){
				for(int i=0; i<list_ttAverage.size(); i++){
					if(i>1 && i < list_ttAverage.size()-2){
						double value = ((1.0/3)*list_ttAverage.get(i-2))+((2.0/3)*list_ttAverage.get(i-1))+list_ttAverage.get(i)+((2.0/3)*list_ttAverage.get(i+1))+((1.0/3)*list_ttAverage.get(i+2));
						value = value / 3;
						list_ttExponentialSmoothing.add((int)Math.round(value));
					}
					else{
						list_ttExponentialSmoothing.add(list_ttAverage.get(i));
					}
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
}

