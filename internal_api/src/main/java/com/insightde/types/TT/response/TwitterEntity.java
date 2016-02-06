package com.insightde.types.TT.response;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Lists;
import com.insightde.types.TT.response.entities.TwitterHashtag;
import com.insightde.types.TT.response.entities.TwitterUserMention;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TwitterEntity {
	
    private List<TwitterHashtag> hashtags;
//    private TwitterUrls urls;
//    private TwitterMedia media;
    
    private List<TwitterUserMention> user_mentions;

    public TwitterEntity() {
    	
    	
//        JSONArray hashtagsJSON = entities.optJSONArray("hashtags");
//        if (hashtagsJSON != null) {
//            hashtags =  TwitterHashtags(hashtagsJSON);
//        } else {
//            hashtags = null;
//        }
//
//        JSONArray urlsJSON = entities.optJSONArray("urls");
//        if (urlsJSON != null) {
//            urls = TwitterUrls(urlsJSON);
//        } else {
//            urls = null;
//        }
//
//        JSONArray MediaJSON = entities.optJSONArray("media");
//        if (MediaJSON != null) {
//            media = TwitterMedia.parseJSON(MediaJSON);
//        } else {
//            media = null;
//        }
    }

	public List<TwitterHashtag> getHashtags() {
		return hashtags;
	}

	public List<String> getHashtagsAsStrings(){
		List<String>hts = Lists.newArrayList();
		for(TwitterHashtag hashtag: hashtags){
			hts.add(hashtag.getText());
		}
		return hts;
	}
	
	public void setHashtags(List<TwitterHashtag> hashtags) {
		this.hashtags = hashtags;
	}

	public List<TwitterUserMention> getUser_mentions() {
		return user_mentions;
	}

	public void setUser_mentions(List<TwitterUserMention> user_mentions) {
		this.user_mentions = user_mentions;
	}
}

/*
    "entities": {
      "hashtags": [],
      "symbols": [],
      "user_mentions": [],
      "urls": [],
      "media": []
    },
 */
