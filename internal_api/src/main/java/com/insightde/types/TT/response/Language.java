package com.insightde.types.TT.response;

import java.util.ArrayList;
import java.util.List;

public enum Language {
	EN("English",	"en")
	,AR("Arabic",	"ar")
	,BN("Bengali",	"bn")
	,CS("Czech",	"cs")
	,DA("Danish",	"da")
	,DE("German","de")
	,EL("Greek",	"el")
	,ES("Spanish",	"es")
	,FA("Persian",	"fa")
	,FI("Finnish",	"fi")
	,FIL("Filipino",	"fil")
	,FR("French","fr")
	,HE("Hebrew","he")
	,HI("Hindi","hi")
	,HU("Hungarian","hu")
	,ID("Indonesian","id")
	,IT("Italian","it")
	,JA("Japanese","ja")
	,KO("Korean","ko")
	,MSA("Malay","msa")
	,NL("Dutch","nl")
	,NO("Norwegian","no")
	,PL("Polish","pl")
	,Pt("Portuguese","pt")
	,RO("Romanian","ro")
	,RU("Russian","ru")
	,SV("Swedish","sv")
	,TH("Thai","th")
	,TR("Turkish","tr")
	,UK("Ukrainian","uk")
	,UR("Urdu","ur")
	,VI("Vietnamese","vi")
	,CN("Chinese (Simplified)","zh-cn")
	,TW("Chinese (Traditional)","zh-tw");
	
	
	private final String shortname;
	
	@SuppressWarnings("unused")
	private  String longname;
	
	private Language(String longname, String shortname){
		this.shortname = shortname;
		this.longname = longname;
	}
	
	public static Language findLanguageByShortName(String shortname){
		for (Language l:Language.values()){
			if (l.shortname.equals(shortname.toLowerCase())){
				return l;
			}
		}return null;
	}
	
	public static List<Language> findLanguages(String csvList){
		String[] splitted = csvList.split(",");
		List<Language> ls = new ArrayList<Language>();
		for (String s:splitted){
			Language l = Language.findLanguageByShortName(s.toLowerCase().trim());
			ls.add(l);
		}
		return ls;
	}
	
	@Override
	public String toString() {
		return this.shortname;
	}
}
