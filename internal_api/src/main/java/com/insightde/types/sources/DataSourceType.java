package com.insightde.types.sources;


public enum DataSourceType {
	
	TT("Twitter", "TT"),
	RD("Reddit","RD"),
	FB("Facebook", "FB");
	

	private final String longname;
	private final String shortname;

	private DataSourceType(String longname, String shortname) {
		this.longname = longname;
		this.shortname = shortname;
	}

	public static DataSourceType findDSbyShortname(String shortname) {
		for (DataSourceType ds : DataSourceType.values()) {
			if (ds.getShortname().equals(shortname.toUpperCase())) {
				return ds;
			}
		}
		return null;
	}

	public String getLongname() {
		return longname;
	}

	public String getShortname() {
		return shortname;
	}
	
}
