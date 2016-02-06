package com.insightde.types.TT.response.place.bounding_box;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Lists;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BoundingBox {
	
	private String type;
	
	private List<List<Object>> coordinates = Lists.newArrayList();

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<List<Object>> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(List<List<Object>> coordinates) {
		this.coordinates = coordinates;
	}
	
//	@Override
//	public String toString() {
//		ObjectMapper mapper = new ObjectMapper();
//		String jsonInString="";
//		try {
//			jsonInString = mapper.writeValueAsString(this);
//		} catch (JsonProcessingException e) {
//			// TODO Auto-generated catch block
//			
//		}
//		return jsonInString;
//	}

}
