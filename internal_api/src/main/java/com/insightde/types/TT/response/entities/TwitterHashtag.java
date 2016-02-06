package com.insightde.types.TT.response.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

//import org.json.JSONArray;
@JsonIgnoreProperties(ignoreUnknown = true)
public class TwitterHashtag {
	private String text;
	
    public TwitterHashtag() {
    }

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}

/*
"hashtags": [
    {
        "text": "BookWorm",
        "indices": [
            111,
            120
        ]
    },
    {
        "text": "CR4U",
        "indices": [
            121,
            126
        ]
    },
    {
        "text": "T4US",
        "indices": [
            127,
            132
        ]
    }
]
 */