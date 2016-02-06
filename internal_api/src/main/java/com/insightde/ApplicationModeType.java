package com.insightde;


public enum ApplicationModeType {
	
	DEV("development", "DEV"),
	UAT("uat", "UAT"),
	PROD("production", "PROD");
	

	private final String longname;
	private final String shortname;

	private ApplicationModeType(String longname, String shortname) {
		this.longname = longname;
		this.shortname = shortname;
	}

	public static ApplicationModeType findByName(String name) {
		for (ApplicationModeType amt : ApplicationModeType.values()) {
			if (amt.getLongname().equals(name.toLowerCase())){
				return amt;
			}
		}
		return null;
	}
	
	public static ApplicationModeType findByShortName(String name) {
		for (ApplicationModeType amt : ApplicationModeType.values()) {
			if (amt.getShortname().equals(name.toUpperCase())){
				return amt;
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
