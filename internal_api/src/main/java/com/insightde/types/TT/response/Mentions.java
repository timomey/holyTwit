package com.insightde.types.TT.response;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class Mentions /*extends Model*/ {
    // Constants
    static final String TWITTER_TIME_FORMAT = "ccc MMM d HH:mm:ss z yyyy";
    static SimpleDateFormat twitterDateFormat = new SimpleDateFormat(TWITTER_TIME_FORMAT, Locale.US);

    // Member Variables
//    @Column(name="tweet_id", index = true, unique = true, onUniqueConflict = Column.ConflictAction.REPLACE)
//    private Long tweet_id;

//    @Column(name="text")
    private String text;

//    @Column(name="created_at")
    private Long created_at;

//    @Column(name="favorite_count")
    private int favorite_count;

//    @Column(name="retweet_count")
    private int retweet_count;

//    @Column(name="favorited")
    private boolean favorited;

//    @Column(name="retweeted")
    private boolean retweeted;

//    @Column(name="user_id")
    @SuppressWarnings("unused")
	private long user_id;

//    Going to simplify this for now.  I'll just use the first image and keep the rest as json strings to parse at runtime
//    @Column(name="entities")
//    private  TwitterEntity entities;

//    @Column(name="urls")
    @SuppressWarnings("unused")
	private String entity_urls;

//    @Column(name="hashtags")
    @SuppressWarnings("unused")
	private String entity_hashtags;

//    @Column(name="media_id")
    @SuppressWarnings("unused")
	private long media_id;

    // Constructors
    public Mentions() {
        super();
    }

//    public Mentions(JSONObject response) throws JSONException {
//        super();
//
//        tweet_id = response.getLong("id");
//        created_at = parse_time(response.getString("created_at"));
//        text = response.getString("text");
//        favorite_count = response.getInt("favorite_count");
//        retweet_count = response.getInt("retweet_count");
//        favorited = response.getBoolean("favorited");
//        retweeted = response.getBoolean("retweeted");
//
//        // User
//        JSONObject userJSON = response.getJSONObject("user");
//        user_id = userJSON.getLong("id");
//
//        TwitterUser user = TwitterUser.getById(user_id);
//        if (user == null) {
//            user = new TwitterUser(userJSON);
//            user.save();
//        }
//
//        // Entities
//        JSONObject entities = response.getJSONObject("entities");
//        JSONArray urlJSON = entities.optJSONArray("urls");
//        entity_urls = urlJSON == null ? "" : urlJSON.toString();
//        JSONArray hashtagJSON = entities.optJSONArray("hashtags");
//        entity_hashtags = hashtagJSON == null ? "" : hashtagJSON.toString();
//
//        // We'll only use the first media object and ignore the rest for now.  Might grab them later to add to larger screens.
//        media_id = 0;
//        JSONArray mediaJSON = entities.optJSONArray("media");
//        if (mediaJSON != null) {
//            JSONObject firstMediaJSON = mediaJSON.optJSONObject(0);
//            if (firstMediaJSON != null) {
//                TwitterMedia twitterMedia = TwitterMedia.getById(firstMediaJSON.getLong("id"));
//                if (twitterMedia == null) {
//                    twitterMedia = new TwitterMedia(firstMediaJSON);
//                    twitterMedia.save();
//                }
//                Log.d("blah", "Added item for " + tweet_id + ": " + twitterMedia.getMedia_url());
//                media_id = twitterMedia.getMedia_id();
//            }
//        }
//    }

    @SuppressWarnings("unused")
	private Long parse_time(String twitterTime) {
        long dateMillis = 0;
        String twitterFormat = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(twitterFormat, Locale.ENGLISH);
        sf.setLenient(true);

        try {
            dateMillis = sf.parse(twitterTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dateMillis;
    }

    // Getters
//    public Long getTweet_id() {
//        return tweet_id;
//    }

    public String getText() {
        return text;
    }

    public Long getCreated_at() {
        return created_at;
    }

//    public String getRelativeTimeCreated() {
//        return DateUtils.getRelativeTimeSpanString(created_at,
//                System.currentTimeMillis(), DateUtils.SECOND_IN_MILLIS).toString();
//    }


    public boolean isFavorited() {
        return favorited;
    }

    public boolean isRetweeted() {
        return retweeted;
    }

    public int getFavorite_count() {
        return favorite_count;
    }

    public int getRetweet_count() {
        return retweet_count;
    }

//    public TwitterUser getUser() {
//        return TwitterUser.getById(user_id);
//    }

//    public TwitterMedia getMedia() {
//        return TwitterMedia.getById(media_id);
//    }

    // Setters

//    public void favorited() {
//        this.favorited = true;
//        this.favorite_count ++;
//        save();
//    }
//
//    public void unFavorited() {
//        this.favorited = false;
//        if (this.favorite_count > 0)
//            this.favorite_count--;
//        save();
//    }
//
//    public void setFavorited(boolean favorited) {
//        this.favorited = favorited;
//        save();
//    }
//
//    public void setRetweeted(boolean retweeted) {
//        this.retweeted = retweeted;
//        save();
//    }
//
//    public void setFavorite_count(int favorite_count) {
//        this.favorite_count = favorite_count;
//        save();
//    }
//
//    public void setRetweet_count(int retweet_count) {
//        this.retweet_count = retweet_count;
//        save();
//    }
//// SQL Gathers
//
//    public static Mentions getById(long id) {
//        return new Select().from(Mentions.class).where("tweet_id = ?", id).executeSingle();
//    }
//
//    public static List<Mentions> recentItems() {
//        return new Select().from(Mentions.class).orderBy("tweet_id DESC").limit("300").execute();
//    }
//
//    public static List<Mentions> itemsAfterId(long max_id) {
//        return new Select()
//                .from(Mentions.class)
//                .where("tweet_id < ?", max_id)
//                .orderBy("tweet_id DESC")
//                .limit("300")
//                .execute();
//    }
//
//    public static List<Mentions> itemsInRange(long min_id, long max_id, String order) {
//        return new Select()
//                .from(Mentions.class)
//                .where("tweet_id <= ?", max_id)
//                .where("tweet_id >= ?", min_id)
//                .orderBy("tweet_id " + order)
//                .limit("300")
//                .execute();
//    }
}

/*
    "created_at": "Tue May 19 07:41:58 +0000 2015",
    "tweet_id": 600567055784529900,
    "id_str": "600567055784529921",
    "text": "edisonsparadise said: Hi Samantha! I LOVED \"The Bone Season\" so thank you for the book, i was wondering if... http://t.co/LEXiME7eJF",
    "source": "<a href=\"http://www.tumblr.com/\" rel=\"nofollow\">Tumblr</a>",
    "truncated": false,
    "in_reply_to_status_id": null,
    "in_reply_to_status_id_str": null,
    "in_reply_to_user_id": null,
    "in_reply_to_user_id_str": null,
    "in_reply_to_screen_name": null,
    "user": {},
    "geo": null,
    "coordinates": null,
    "place": null,
    "contributors": null,
    "retweet_count": 0,
    "favorite_count": 2,
    "entities": {
      "hashtags": [],
      "symbols": [],
      "user_mentions": [],
      "urls": [],
      "media": []
     },
    "favorited": false,
    "retweeted": false,
    "possibly_sensitive": false,
    "possibly_sensitive_appealable": false,
    "lang": "en"
 */