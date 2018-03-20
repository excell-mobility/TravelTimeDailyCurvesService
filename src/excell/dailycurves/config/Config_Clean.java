
package excell.dailycurves.config;

/**
 * Konfiguration
 */
public class Config_Clean {
	
  	/**Start of the calculation of the hydrographes (hour of the day)*/
  	public static int startCalculation = 1;
  	  	
  	/**maximum number of archived travel time values within an interval (15 minutes)*/
  	public static int maxtt_per_interval = 20;
  	
  	/**maximum number of archived sensor data within one interval (15 minutes)*/
  	public static int maxSensorData_per_interval = 10;
  	
  	/**alpha value of the exponential smoothing of the average value of an interval*/
  	public static double alpha = 0.2;
  	
  	/**Size of the gap of missing values in interpolation of the hydrograph*/
  	public static int numMissingQuarter = 3;
  	  	  	  			      
	/** access data postgreSQL excell */
	public static String excelldb_host = "";
  	public static String excelldb_port = "";
  	public static String excelldb_user = "";
  	public static String excelldb_pass = "";
  	public static String excelldb_name = "";
  	
  	  	
  	/** tables */
  	public static String table_raw_edge_records = "";
  	public static String table_raw_edge_records_tt_archiv = "";
  	public static String table_tt_hydrograph_edges = "";
  	
  	public static String table_raw_sensor_data = "";
  	public static String table_raw_sensor_data_speed_archiv = "";
  	public static String table_proc_sensor_data_speed_hydrograph = "";
  	
  	
  	/** time interval for recalculation of hydrographs */
  	public static int timerHydrographs = 24*60*60*1000; //in ms
  	  	
		
}
