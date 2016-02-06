package com.insightde.types.TT.response.entities;

//import com.activeandroid.Model;

//import org.json.JSONException;
//import org.json.JSONObject;

import java.util.Map;

//@Table(name="TwitterMedia")
public class TwitterMedia /*extends Model*/ {
    // Member Functions

//    @Column(name="media_id", index = true, unique = true, onUniqueConflict = Column.ConflictAction.REPLACE)
    private long media_id;

//    @Column(name="media_url")
    private String media_url;

//    @Column(name="url")
    private String url;

//    @Column(name="type")
    private String type;

//    @Column(name="sizes")
    private Map<String, MediaSize> sizes;

    // Constructor

    public TwitterMedia() { super(); }
//    public TwitterMedia(JSONObject mediaObjJSON) throws JSONException {
//        media_id = mediaObjJSON.getLong("id");
//        media_url = mediaObjJSON.getString("media_url");
//        url = mediaObjJSON.getString("url");
//        type = mediaObjJSON.getString("type");
//
//        sizes = new HashMap<>();
//        JSONObject sizesJSON = mediaObjJSON.getJSONObject("sizes");
//        Iterator<String> iter = sizesJSON.keys();
//        while (iter.hasNext()) {
//            String key = iter.next();
//            JSONObject size = sizesJSON.getJSONObject(key);
//            MediaSize mediaSize = new MediaSize();
//            mediaSize.w = size.getInt("w");
//            mediaSize.h = size.getInt("h");
//            mediaSize.resize = size.getString("resize");
//
//            sizes.put(key, mediaSize);
//        }
//    }

    // Getters

    public long getMedia_id() {
        return media_id;
    }

    public String getMedia_url() {
        return media_url;
    }

    public String getUrl() {
        return url;
    }

    public String getType() {
        return type;
    }

    public Map<String, MediaSize> getSizes() {
        return sizes;
    }

//    public static TwitterMedia getById(long id) {
//        return new Select().from(TwitterMedia.class).where("media_id = ?", id).executeSingle();
//    }


    // Parser Multiples
//    static public String parseJSON(JSONArray mediaJSON) throws JSONException {
//        ArrayList<TwitterMedia> mediaList = new ArrayList<>();
//        for (int i = 0; i < mediaJSON.length() ; i++) {
//            mediaList.add(new TwitterMedia(mediaJSON.getJSONObject(i)));
//        }
//
//        return mediaList;
//    }
}

/**
 * MediaSize for media objects
 */
class MediaSize {
    public int h;
    public int w;
    public String resize;
}

/*
"media": [
    {
      "id": 600557020715036672,
      "id_str": "600557020715036672",
      "indices": [
        93,
        115
      ],
      "media_url": "http://pbs.twimg.com/media/CFWbba8WMAAdJ4S.jpg",
      "media_url_https": "https://pbs.twimg.com/media/CFWbba8WMAAdJ4S.jpg",
      "url": "http://t.co/hr4Ky1XlXa",
      "display_url": "pic.twitter.com/hr4Ky1XlXa",
      "expanded_url": "http://twitter.com/SFGate/status/600557021058961408/photo/1",
      "type": "photo",
      "sizes": {
        "large": {
          "w": 810,
          "h": 545,
          "resize": "fit"
        },
        "medium": {
          "w": 600,
          "h": 403,
          "resize": "fit"
        },
        "thumb": {
          "w": 150,
          "h": 150,
          "resize": "crop"
        },
        "small": {
          "w": 340,
          "h": 228,
          "resize": "fit"
        }
      }
    }
  ]
 */