package com.insightde.taggers.dao;

import java.util.List;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Rule {
	
//	private static final  Config conf = ConfigFactory.load();
//	private static final int maxNumRules = conf.getInt("tagging.rulesset.size");
	
	private String mainTheme = null;
	private String subTheme = null;
	private String rule = null;
	
	public String getRule() {
		return rule;
	}

	public void setRule(String rule) {
		this.rule = rule;
	}

	private List<String>rules = Lists.newArrayList();
	private List<String>filenames = Lists.newArrayList();
	
	private String comments = null;
	
	public Rule(String mainTheme, String subTheme){
		this.mainTheme  = mainTheme;
		this.subTheme = subTheme;
	}

	public String getMainTheme() {
		return mainTheme;
	}

	public void setMainTheme(String mainTheme) {
		this.mainTheme = mainTheme;
	}

	public String getSubTheme() {
		return subTheme;
	}

	public void setSubTheme(String subTheme) {
		this.subTheme = subTheme;
	}

	public List<String> getRules() {
		return rules;
	}

	public void setRules(List<String> rules) {
		this.rules = rules;
	}

	public List<String> getFilenames() {
		return filenames;
	}

	public void setFilenames(List<String> filenames) {
		this.filenames = filenames;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}
}
