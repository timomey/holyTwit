package com.insightde.types.TT.response;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.insightde.types.GenericPost;
import com.insightde.types.TT.response.place.Place;
import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet implements GenericPost{
	private static final Logger logger = LoggerFactory.getLogger(Tweet.class);
	
	private static ObjectMapper mapper = new ObjectMapper();

	private static Config conf = ConfigFactory.load();
	private static final SimpleDateFormat logstashDateFormat = new SimpleDateFormat(conf.getString("dateformat.logstash")); //'%Y-%m-%dT%H:%M:%S%z'

	private final static String textFormat = conf.getString("text.format");

	// Constants
	static final String TWITTER_TIME_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
	static SimpleDateFormat twitterDateFormat = new SimpleDateFormat(TWITTER_TIME_FORMAT, Locale.US);

	// Member Variables
	public Long id;
	public String id_str;
	private String body;
	private Date created_at;
	private Language lang;
	private Place place;
	private List<String> pharmatags = Lists.newArrayList();
	private List<String> symptomtags = Lists.newArrayList();
	private Set<String> conditiontags = Sets.newHashSet();
	private long timestamp_ms = 0L;
	private String user_screen_name;
	
	public Date getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		try {
			this.created_at = twitterDateFormat.parse(created_at);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public String getBody(){
		return body;
	}

	public void setText(String text) {
		this.body = text.replaceAll(textFormat, " ");
	}
	public String getIdAsStr(){
		if(getId_str()!=null){
			return getId_str();
		}
		return id.toString();
		
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

//	public int getFavorite_count() {
//		return favorite_count;
//	}
//
//	public void setFavorite_count(int favorite_count) {
//		this.favorite_count = favorite_count;
//	}
//
//	public int getRetweet_count() {
//		return retweet_count;
//	}
//
//	public void setRetweet_count(int retweet_count) {
//		this.retweet_count = retweet_count;
//	}
//
//	public boolean isFavorited() {
//		return favorited;
//	}
//
//	public void setFavorited(boolean favorited) {
//		this.favorited = favorited;
//	}
//
//	public boolean isRetweeted() {
//		return retweeted;
//	}
//
//	public void setRetweeted(boolean retweeted) {
//		this.retweeted = retweeted;
//	}
//
//	public TwitterEntity getEntities() {
//		return entities;
//	}
//
//	public void setEntities(TwitterEntity entities) {
//		this.entities = entities;
//	}

//	public TwitterUser getUser() {
//		return user;
//	}
//	public void setUser(TwitterUser user) {
//		this.user = user;
//	}
	
	@Override
	public String toString(){
		ObjectMapper mapper = new ObjectMapper();
		mapper.setSerializationInclusion(Include.NON_NULL);
		String jsonInString="";
		try {
			jsonInString = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
		}
		return jsonInString;
	}

//	@Override
//	public String toString() {
//		StringBuilder result = new StringBuilder();
//		String newLine = System.getProperty("line.separator");
//		result.append(this.getClass().getName());
//		result.append(" Object {");
//		result.append(newLine);
//		Field[] fields = this.getClass().getDeclaredFields();
//
//		// print field names paired with their values
//		for (Field field : fields) {
//			result.append("  ");
//			try {
//				result.append(field.getName());
//				result.append(": ");
//				result.append(field.get(this));
//			} catch (IllegalAccessException ex) {
//				System.out.println(ex);
//			}
//			result.append(newLine);
//		}
//		result.append("}");
//		return result.toString();
//	}

	public Language getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = Language.findLanguageByShortName(lang);
	}

	public Place getPlace() {
		return place;
	}

	public void setPlace(Place place) {
		this.place = place;
	}

	// truncated version of original post for indexing
//	public  Map<String, Object> transformTT() {
//		Map<String, Object> data = new HashMap<String, Object>();
//		Map<String, Object> place = new HashMap<String, Object>();
//		Map<String, Object> bounding_box = new HashMap<String, Object>();
////		List<String> hashtags = this.getEntities().getHashtagsAsStrings();
//		data.put("user_id", this.getUser().getScreen_name());
//		data.put("post_id", this.getId());
//		data.put("@timestamp", logstashDateFormat.format(this.getCreated_at()));
//		data.put("created_utc", this.getCreated_utc());
//		data.put("message", this.getText());
//		data.put("lang", this.getLang().toString());
////		if (!hashtags.isEmpty()) {
////			data.put("hashtags", hashtags);
////		}
//		if(!this.getTags().isEmpty()){
//			data.put("tags", this.getTags());
//		}
//		if (this.getPlace() != null) {
//			bounding_box.put("type", this.getPlace().getBounding_box().getType());
//			bounding_box.put("coordinates", this.getPlace().getBounding_box().getCoordinates());
//			place.put("id", this.getPlace().getId());
//			place.put("bounding_box", bounding_box);
//			place.put("name", this.getPlace().getName());
//			place.put("country_code", this.getPlace().getCountry_code());
//			data.put("place", place);
//		}
//		return data;
//	}
	
	public String getId_str() {
		return id_str;
	}

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}



	
	// sometimes coordinates are not correct coming from Twitter
	@SuppressWarnings("unchecked")
	public static boolean validCoordinates(Tweet tweet) {
		if (tweet == null) {
			return true;
		}
		if (tweet.getPlace() != null) {
			if (tweet.getPlace().getBounding_box().getCoordinates().size() != 1) {
				return true;
			} else {
				List<Object> coordinates = tweet.getPlace().getBounding_box().getCoordinates().get(0);

				if (coordinates.size() != 4) {
					return true;
				} else {
					Iterator<Object> iterator = coordinates.iterator();
					List<Object> previous = (List<Object> )iterator.next();
					while (iterator.hasNext()) {
						List<Object> current = (List<Object> ) iterator.next();
						for (int i = 0; i < previous.size(); i++) {
							if (!previous.get(i).equals(current.get(i))) {
								return true;
							}
						}
						previous = current;
					}
					return false;
				}
			}
		}
		return true;
	}
	
	public static Tweet jsonToPojo(String msg) {
		mapper = new ObjectMapper();
		try {
			Tweet tweet = mapper.readValue(msg, Tweet.class);
			if (!validCoordinates(tweet)) {
				tweet.getPlace().setBounding_box(null);
			}
			return tweet;
		} catch (IOException e) {
			logger.info(e.getMessage());
		}
		return null;
	}
	
	public Long getTimestamp_ms(){
		return timestamp_ms;
	}
	
	public void setTimestamp_ms(Long timestamp_ms){
		this.timestamp_ms = timestamp_ms;
	}
	

	public List<String> getPharmatags() {
//		if (pharmatags.size()==0) return null;
		return pharmatags;
	}

	public void setPharmatags(List<String> tags) {
		this.pharmatags = tags;
	}
	
	@Override
	public List<String> getSymptomtags() {
//		if (symptomtags.size()==0) return null;
		return symptomtags;
	}

	@Override
	public void setSymptomtags(List<String> symptomtags) {
		this.symptomtags = symptomtags;
		
	}
	
	@Override
	public Set<String> getConditiontags() {
//		if (conditiontags.size()==0) return null;
		return conditiontags;
	}

	@Override
	public void setConditiontags(Set<String> conditiontags) {
		this.conditiontags = conditiontags;
		
	}

	public String getUser_screen_name() {
		return user_screen_name;
	}

	public void setUser_screen_name(String user_screen_name) {
		this.user_screen_name = user_screen_name;
	}

//	public long getTimestamp_ms() {
//		return timestamp_ms;
//	}
//
//	public void setTimestamp_ms(long timestamp_ms) {
//		this.timestamp_ms = timestamp_ms;
//	}

}

/*
 * "created_at": "Tue May 19 07:41:58 +0000 2015", "tweet_id":
 * 600567055784529900, "id_str": "600567055784529921", "text":
 * "edisonsparadise said: Hi Samantha! I LOVED \"The Bone Season\" so thank you for the book, i was wondering if... http://t.co/LEXiME7eJF"
 * , "source": "<a href=\"http://www.tumblr.com/\" rel=\"nofollow\">Tumblr</a>",
 * "truncated": false, "in_reply_to_status_id": null,
 * "in_reply_to_status_id_str": null, "in_reply_to_user_id": null,
 * "in_reply_to_user_id_str": null, "in_reply_to_screen_name": null, "user": {},
 * "geo": null, "coordinates": null, "place": null, "contributors": null,
 * "retweet_count": 0, "favorite_count": 2, "entities": { "hashtags": [],
 * "symbols": [], "user_mentions": [], "urls": [], "media": [] }, "favorited":
 * false, "retweeted": false, "possibly_sensitive": false,
 * "possibly_sensitive_appealable": false, "lang": "en"
 */