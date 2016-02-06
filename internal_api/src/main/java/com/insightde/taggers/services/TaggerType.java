package com.insightde.taggers.services;

public enum TaggerType {
	
	LUWAK("luwak");
	
	private final String longname;
	
	public String getLongname() {
		return longname;
	}

	private TaggerType(String longname){
		this.longname = longname ;
	}
	
	public static TaggerType findByName(String name){
		for (TaggerType t: TaggerType.values()){
			if (name.toLowerCase().equals(t.getLongname())){
				return t;
			}
		}return null;
	}

}
