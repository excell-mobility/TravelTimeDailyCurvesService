package excell.dailycurves;

import java.util.Timer;

import excell.dailycurves.config.Config;

public class MainTravelTimeDailyCurvesService {

	public static void main(String[] args) {
					
		Thread threadCalculateHydrographs = new Thread();
		Timer timerCalculateHydrographs = new Timer();
		int delayTagesganglinie = Function.bestimmeMillisBisMitternacht() + (Config.startCalculation*60*60*1000);
		
		timerCalculateHydrographs.schedule(threadCalculateHydrographs, delayTagesganglinie, Config.timerHydrographs);

		//test
		//timerCalculateHydrographs.schedule(threadTagesganglinienErzeugen, 10000, Config.timerTagesganglinien);

	}

}
