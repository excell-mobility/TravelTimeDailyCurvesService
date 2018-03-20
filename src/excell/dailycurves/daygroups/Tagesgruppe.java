package excell.dailycurves.daygroups;

public abstract class Tagesgruppe {
	
	public static final int MONTAG=11;
	public static final int DIENSTAG=12;
	public static final int MITTWOCH=13;
	public static final int DONNERSTAG=14;
	public static final int FREITAG=15;
	public static final int SAMSTAG=16;
	public static final int SONNTAG=17;
	
	public static final int FEIERTAG=20;
	public static final int BRUECKENTAG=30;
	
	public static final int MONTAG_SCHULFERIEN=41;
	public static final int DIENSTAG_SCHULFERIEN=42;
	public static final int MITTWOCH_SCHULFERIEN=43;
	public static final int DONNERSTAG_SCHULFERIEN=44;
	public static final int FREITAG_SCHULFERIEN=45;
	public static final int SAMSTAG_SCHULFERIEN=46;
	public static final int SONNTAG_SCHULFERIEN=47;
	
	public static final int FERIENBEGINN=50;	
	
	public static final String hilfstabelle="fcd_tmp";
		
	public abstract int getTagesgruppenId();

}
