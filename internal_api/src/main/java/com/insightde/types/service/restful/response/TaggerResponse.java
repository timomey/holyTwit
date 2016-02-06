package com.insightde.types.service.restful.response;

import static org.apache.log4j.Logger.getLogger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insightde.types.TT.response.Tweet;
import com.insightde.types.reddit.Reddit;
import com.insightde.types.sources.DataSourceType;

public class TaggerResponse {
	
	private static ObjectMapper mapper = new ObjectMapper();
	private static final Logger logger = getLogger(TaggerResponse.class);
    
	private String status;
    private Map<String,Reddit> taggerRespReddit =null;
    private Map<String,Tweet> taggerRespTweet = null;
    private DataSourceType dataSourceType = null;
    
     
    public String getStatus() {
		return status;
	}
    public void setStatus(String status) {
		this.status = status;
	}
    public List< Reddit> getTaggerRespReddit() {
    	return new ArrayList<Reddit>(taggerRespReddit.values());
	}

	public void setTaggerRespReddit(Map<String, Reddit> taggerResp) {
		this.taggerRespReddit = taggerResp;
	}
	
    public List< Tweet> getTaggerRespTweet() {
    	return new ArrayList<Tweet>(taggerRespTweet.values());
	}

	public void setTaggerRespTweet(Map<String, Tweet> taggerResp) {
		this.taggerRespTweet = taggerResp;
	}

	public TaggerResponse(DataSourceType ds){
    	this.dataSourceType = ds;
    }
	
	
	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		if(dataSourceType==DataSourceType.RD){
		
			List<Reddit> enrichedReddits= getTaggerRespReddit();
			StringBuilder sb = new StringBuilder();
			int count =0;
			for(Reddit reddit: enrichedReddits){
				if(count>0)
					sb.append("\n");
				sb.append(reddit.toString());
				count+=1;
			}
			return sb.toString();
		}else if(dataSourceType==DataSourceType.TT){
			List<Tweet> enrichedTweets= getTaggerRespTweet();
			StringBuilder sb = new StringBuilder();
			int count =0;
			for(Tweet tweet: enrichedTweets){
				if(count>0)
					sb.append("\n");
				sb.append(tweet.toString());
				count+=1;
				return sb.toString();
			}
		}return "";
	}
	
	public static TaggerResponse jsonToPojo(String json){
		TaggerResponse tr=null;
		try {
			tr = mapper.readValue(json, TaggerResponse.class);
		} catch (JsonParseException e) {
			logger.error(e.toString());
		} catch (JsonMappingException e) {
			logger.error(e.toString());
		} catch (IOException e) {
			logger.error(e.toString());
		}
		return tr;
	}
	public void setDataSourceType(DataSourceType dataSourceType) {
		this.dataSourceType = dataSourceType;
	}

}
