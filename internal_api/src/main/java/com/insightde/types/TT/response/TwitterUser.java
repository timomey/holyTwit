package com.insightde.types.TT.response;



import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TwitterUser /*extends Model */{
	
    private long id;

//    @Column(name="name")
    private String name;

//    @Column(name="screen_name")
    private String screen_name;

//    @Column(name="location")
    private String location;

//    @Column(name="description")
    private String description;
    
    private boolean verified;
    
    private long followers_count;
    private long friends_count;
    private long favourites_count;
    private boolean geo_enabled;
    private Language lang;

//    @Column(name="profile_image_url")
//    private String profile_image_url;

//    @Column(name="url")
//    private String url;

//    @Column(name="entities")
//    private String entities;


    // Constructors


    public TwitterUser() {  }

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getScreen_name() {
		return screen_name;
	}

	public void setScreen_name(String screen_name) {
		this.screen_name = screen_name;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public boolean isVerified() {
		return verified;
	}

	public void setVerified(boolean verified) {
		this.verified = verified;
	}

	public long getFollowers_count() {
		return followers_count;
	}

	public void setFollowers_count(long followers_count) {
		this.followers_count = followers_count;
	}

	public long getFriends_count() {
		return friends_count;
	}

	public void setFriends_count(long friends_count) {
		this.friends_count = friends_count;
	}

	public long getFavourites_count() {
		return favourites_count;
	}

	public void setFavourites_count(long favourites_count) {
		this.favourites_count = favourites_count;
	}

	public boolean isGeo_enabled() {
		return geo_enabled;
	}

	public void setGeo_enabled(boolean geo_enabled) {
		this.geo_enabled = geo_enabled;
	}

	public Language getLang() {
		return lang;
	}

	public void setLang(String lang) {
		this.lang = Language.findLanguageByShortName(lang);
	}
	
	


//    public TwitterUser(JSONObject userJSON) throws JSONException {
//        super();
//
//        user_id = userJSON.getLong("id");
//        name = userJSON.getString("name");
//        screen_name = userJSON.getString("screen_name");
//        location = userJSON.getString("location");
//        description = userJSON.getString("description");
//        profile_image_url = userJSON.getString("profile_image_url");
//
//        url = userJSON.getString("url");
//        entities = userJSON.getJSONObject("entities").toString();
//    }


    // SQL Accessors
//    public static TwitterUser getById(long id) {
//        return new Select().from(TwitterUser.class).where("user_id = ?", id).executeSingle();
//    }


//    public static List<Tweet> recentUsers() {
//        return new Select().from(TwitterUser.class).orderBy("user_id DESC").limit("300").execute();
//    }

//    public Bitmap getBitmap(Context context) {
//        return TwitterImage.getImage(this, context);
//    }
}


/*
"user": {
  "id": 7861312,
  "id_str": "7861312",
  "name": "Felicia Day",
  "screen_name": "feliciaday",
  "location": "Los Angeles, CA",
  "description": "Actress, New Media Geek, Gamer, Misanthrope. I like to keep my Tweets real and not waste people's time.",
  "url": "http://t.co/Ux6UdnRk8F",
  "entities": {
    "url": {
      "urls": [
        {
          "url": "http://t.co/Ux6UdnRk8F",
          "expanded_url": "http://www.feliciaday.com",
          "display_url": "feliciaday.com",
          "indices": [
            0,
            22
          ]
        }
      ]
    },
    "description": {
      "urls": []
    }
  },
*/