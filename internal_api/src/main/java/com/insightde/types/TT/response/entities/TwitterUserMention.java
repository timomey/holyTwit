package com.insightde.types.TT.response.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

//import org.json.JSONArray;
@JsonIgnoreProperties(ignoreUnknown = true)
public class TwitterUserMention {
	private String screen_name;
	
    public TwitterUserMention() {
    }

	public String getScreen_name() {
		return screen_name;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

}

/*
"user_mentions": [
{
	"screen_name": "FlirtyNotes",
	"name": "Fuck Feelings",
	"id": 623584562,
	"id_str": "623584562",
	"indices": [
		3,
		15
	]
}
],*/
