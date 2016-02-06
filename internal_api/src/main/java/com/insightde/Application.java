package com.insightde;

import java.io.IOException;
//import java.text.SimpleDateFormat;
//import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableScheduling;
//import com.typesafe.config.Config;
//import com.typesafe.config.ConfigFactory;

@SpringBootApplication
@Async
@EnableScheduling
public class Application {
//	private static final Config conf = ConfigFactory.load();
//	private static final String simpleDateFormatString = conf.getString("dateformat.simple");
//	private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(simpleDateFormatString);

	public Application(){
	}
	
	public static void main(String[] args) throws IOException  {
    	SpringApplication.run(Application.class, args);
    }
	// {"created_at":"Mon Feb 01 03:31:12 +0000 2016","id":694000017242218496,"id_str":"694000017242218496","text":"\"Do you want the NEW iPhone 6S?\" #iphone6 #newyork #iphone #california #applewatch #virginia #iPhonegiveaway https://t.co/2ssUjX0lVs","source":"<a href=\"http://ifttt.com\" rel=\"nofollow\">IFTTT</a>","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user_id":3315748004,"user_id_str":"3315748004","user_name":"iphone6s","user_screen_name":"iphone6sbonanza","user_location":null,"user_url":null,"user_description":"I Love My Country, I Love People  And I Love Technology","user_protected":false,"user_verified":false,"user_followers_count":96,"user_friends_count":235,"user_listed_count":64,"user_favourites_count":749,"user_statuses_count":58623,"user_created_at":"Sat Aug 15 07:27:54 +0000 2015","user_utc_offset":-28800,"user_time_zone":"Pacific Time (US & Canada)","user_geo_enabled":false,"user_lang":"en","user_contributors_enabled":false,"user_is_translator":false,"user_profile_background_color":"C0DEED","user_profile_background_image_url":"http://abs.twimg.com/images/themes/theme1/bg.png","user_profile_background_image_url_https":"https://abs.twimg.com/images/themes/theme1/bg.png","user_profile_background_tile":false,"user_profile_link_color":"0084B4","user_profile_sidebar_border_color":"C0DEED","user_profile_sidebar_fill_color":"DDEEF6","user_profile_text_color":"333333","user_profile_use_background_image":true,"user_profile_image_url":"http://pbs.twimg.com/profile_images/655708942316630016/7NwjHaAH_normal.png","user_profile_image_url_https":"https://pbs.twimg.com/profile_images/655708942316630016/7NwjHaAH_normal.png","user_profile_banner_url":"https://pbs.twimg.com/profile_banners/3315748004/1445168091","user_default_profile":true,"user_default_profile_image":false,"user_following":null,"user_follow_request_sent":null,"user_notifications":null,"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,"entities_hashtags":[{"text":"iphone6","indices":[33,41]},{"text":"newyork","indices":[42,50]},{"text":"iphone","indices":[51,58]},{"text":"california","indices":[59,70]},{"text":"applewatch","indices":[71,82]},{"text":"virginia","indices":[83,92]},{"text":"iPhonegiveaway","indices":[93,108]}],"entities_urls":[{"url":"https://t.co/2ssUjX0lVs","expanded_url":"http://goo.gl/Il35J6","display_url":"goo.gl/Il35J6","indices":[109,132]}],"entities_user_mentions":[],"entities_symbols":[],"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en","timestamp_ms":"1454297472664"}
	// {"subreddit_id":"t5_2yljs","score":0,"edited":false,"name":"t1_cekta7f","author_flair_css_class":"default","author":"Kimera25","parent_id":"t1_ceks74z","link_id":"t3_1uqrin","retrieved_on":1431859483,"score_hidden":false,"author_flair_text":"360 100%","subreddit":"chiliadmystery","downs":0,"removal_reason":null,"controversiality":0,"id":"cekta7f","ups":0,"gilded":0,"distinguished":null,"body":"I just tried it and nothing, it's a half moon on a tuesday, no rain, i'll try it with a thunderstorm next time. this got me thinking that the mural could have been painted by the Altruists and each of the X's represents a sacrifice and you have to do the fifth one, I guess on chop. random theory built off all this.","archived":true}
}
