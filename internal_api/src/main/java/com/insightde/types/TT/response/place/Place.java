package com.insightde.types.TT.response.place;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insightde.types.TT.response.place.bounding_box.BoundingBox;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Place {

	private String id;
	private String place_type;
	private String name;
	private String country_code;
	private BoundingBox bounding_box;
	
	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		String jsonInString="";
		try {
			jsonInString = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
		}
		return jsonInString;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPlace_type() {
		return place_type;
	}
	public void setPlace_type(String place_type) {
		this.place_type = place_type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCountry_code() {
		return country_code;
	}
	public void setCountry_code(String country_code) {
		this.country_code = country_code;
	}
	public BoundingBox getBounding_box() {
		return bounding_box;
	}
	public void setBounding_box(BoundingBox bounding_box) {
		this.bounding_box = bounding_box;
	}
}
