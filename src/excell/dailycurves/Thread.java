package excell.dailycurves;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

import excell.dailycurves.achiv.ArchivingEdgeRecords;
import excell.dailycurves.achiv.ArchivingSensorData;
import excell.dailycurves.calculation.CalculationHydrographEdgeRecords;
import excell.dailycurves.calculation.CalculationHydrographSensorData;


public class Thread extends TimerTask{
	
	private SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
			
	private CalculationHydrographEdgeRecords calculationHydrographEdgeRecords;
	private CalculationHydrographSensorData calculationHydrographSensorData;
	private ArchivingEdgeRecords archivingEdgeRecords;
	private ArchivingSensorData archivingSensordData;

	public Thread() {
		System.out.println("MainTravelTimeDailyCurvesService am "+sdf.format(new Date())+" gestartet" );
		//Reisezeit Tagesganglinien
		calculationHydrographEdgeRecords = new CalculationHydrographEdgeRecords();
		calculationHydrographSensorData = new CalculationHydrographSensorData();
		archivingEdgeRecords = new ArchivingEdgeRecords();
		archivingSensordData = new ArchivingSensorData();
	}

	@Override
	public void run() {
		long time = System.currentTimeMillis();
		
		System.out.println("*************  Creation of new daily curves  "+sdf.format(new Date())+"  *************");
		
		//archiv fcd
		System.out.println("archive traveltime from fcd ...");
		archivingEdgeRecords.start();
		System.out.println("archiving finished ("+((System.currentTimeMillis()-time)/1000)+" s) ");
		
		time = System.currentTimeMillis();
		
		//archiv sensordata
		System.out.println("archive sensordata...");
		archivingSensordData.start();
		System.out.println("archiving finished ("+((System.currentTimeMillis()-time)/1000)+" s) ");
				
		time = System.currentTimeMillis();
		
		//traveltime hydrograph calculation
		System.out.println("calculate new edge-records-hydrograph ...");
		calculationHydrographEdgeRecords.start();
		System.out.println("calculate finished ("+((System.currentTimeMillis()-time)/1000)+" s)");
		
		time = System.currentTimeMillis();
		
		//speed-value hydrograph calculation
		System.out.println("calculate new sensor-data-hydrograph...");
		calculationHydrographSensorData.start();
		System.out.println("calculate finished ("+((System.currentTimeMillis()-time)/1000)+" s)");
		
		System.out.println("**********************************************************************************");
		System.out.println();
	}

}
