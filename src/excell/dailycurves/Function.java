package excell.dailycurves;

import java.util.Calendar;

public class Function {
	

	public static int bestimmeMillisBisMitternacht() {
		Calendar jetzt=Calendar.getInstance();
		Calendar heuteMitternacht=Calendar.getInstance();
		heuteMitternacht.add(Calendar.HOUR_OF_DAY, 24);
		heuteMitternacht.set(Calendar.HOUR_OF_DAY, 0);
		heuteMitternacht.set(Calendar.MINUTE, 0);
		heuteMitternacht.set(Calendar.SECOND, 0);
		heuteMitternacht.set(Calendar.MILLISECOND, 0);
		return (int) (heuteMitternacht.getTimeInMillis()-jetzt.getTimeInMillis()+1);
	}
	
	public static int determineDailyGroup(Calendar cal) {
		//TODO:
		int daygroup = 99;
		return daygroup;
	}
	
	public static int determineDailyGroupWithoutHolidays(Calendar cal) {
		//day of week: from 1-sunday until 7-saturday
		int daygroup = cal.get(Calendar.DAY_OF_WEEK);
		return daygroup;
	}

}
