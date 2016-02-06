package com.insightde.types;

import java.util.List;
import java.util.Set;

public interface GenericPost {
	
	public List<String> getPharmatags();
	public void setPharmatags(List<String> pharmatags);
	
	public List<String> getSymptomtags();
	public void setSymptomtags(List<String> symptomtags);
	
	public Set<String> getConditiontags();
	public void setConditiontags(Set<String> conditiontags);
	
	
	public String getBody();
	public String getIdAsStr();
}
