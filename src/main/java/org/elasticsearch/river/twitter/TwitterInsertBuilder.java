package org.elasticsearch.river.twitter;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.JSONObject;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;

public class TwitterInsertBuilder {

	public static XContentBuilder constructInsertBuilder(Status status, boolean autoGenerateGeoPointFromPlace, boolean geoAsArray) throws IOException{
		String location = null;
        
        if(status.getGeoLocation() != null){
    		double latitude = status.getGeoLocation().getLatitude();
    		double longitude = status.getGeoLocation().getLongitude();
    		location = latitude +"," + longitude;
    	}
        
        if(location == null && status.getPlace() != null && autoGenerateGeoPointFromPlace)
        	location = generateGeoPointFromPlace(status);
        
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        
        //root informations
        builder.field("location", location);
        builder.field("created_at", status.getCreatedAt());
        builder.field("id", status.getId());
        builder.field("text", status.getText());
        builder.field("source", status.getSource());
        builder.field("truncated", status.isTruncated());
        builder.field("in_reply_to_status_id", status.getInReplyToStatusId());
        builder.field("in_reply_to_user_id", status.getInReplyToUserId());
        builder.field("in_reply_to_screen_name", status.getInReplyToScreenName());
        builder.field("contributors", status.getContributors());
        builder.field("retweet_count", status.getRetweetCount());
        builder.field("favorite_count", status.getFavoriteCount());
        builder.field("favorited", status.isFavorited());
        builder.field("retweeted", status.isRetweeted());
        builder.field("possibly_sensitive", status.isPossiblySensitive());
        builder.field("lang", status.getLang());
        builder.field("timestamp_ms", System.currentTimeMillis());

        //user
        builder.startObject("user");
        builder.field("id", status.getUser().getId());
        builder.field("name", status.getUser().getName());
        builder.field("screen_name", status.getUser().getScreenName());
        builder.field("location", status.getUser().getLocation());
        builder.field("url", status.getUser().getURL());
        builder.field("description", status.getUser().getDescription());
        builder.field("followers_count", status.getUser().getFollowersCount());
        builder.field("friends_count", status.getUser().getFriendsCount());
        builder.field("listed_count", status.getUser().getListedCount());
        builder.field("favourites_count", status.getUser().getFavouritesCount());
        builder.field("statuses_count", status.getUser().getStatusesCount());
        builder.field("created_at", status.getUser().getCreatedAt());
        builder.field("utc_offset", status.getUser().getUtcOffset());
        builder.field("time_zone", status.getUser().getTimeZone());
        builder.field("geo_enabled", status.getUser().isGeoEnabled());
        builder.field("lang", status.getUser().getLang());
        builder.field("profile_background_image_url", status.getUser().getProfileBackgroundImageURL());
        builder.field("profile_image_url", status.getUser().getProfileImageURL());
        builder.field("profile_banner_url", status.getUser().getProfileBannerURL());
        
        //Place
        if (status.getPlace() != null) {
            builder.startObject("place");
            builder.field("id", status.getPlace().getId());
            builder.field("url", status.getPlace().getURL());
            builder.field("place_type", status.getPlace().getPlaceType());
            builder.field("name", status.getPlace().getName());
            builder.field("full_name", status.getPlace().getFullName());
            builder.field("country_code", status.getPlace().getCountryCode());
            builder.field("country", status.getPlace().getCountry());
            builder.field("street_address", status.getPlace().getStreetAddress());
            builder.startObject("bounding_box").field("type", "envelope").field("coordinates", getEnvelopeFromPlace(status.getPlace().getBoundingBoxCoordinates())).endObject();
            
            builder.endObject();
        }
        
        if (status.getUserMentionEntities() != null) {
            builder.startArray("mention");
            for (UserMentionEntity user : status.getUserMentionEntities()) {
                builder.startObject();
                builder.field("id", user.getId());
                builder.field("name", user.getName());
                builder.field("screen_name", user.getScreenName());
                builder.field("start", user.getStart());
                builder.field("end", user.getEnd());
                builder.endObject();
            }
            builder.endArray();
        }

        if (status.getRetweetCount() != -1) {
            builder.field("retweet_count", status.getRetweetCount());
        }

        if (status.isRetweet() && status.getRetweetedStatus() != null) {
            builder.startObject("retweet");
            builder.field("id", status.getRetweetedStatus().getId());
            if (status.getRetweetedStatus().getUser() != null) {
                builder.field("user_id", status.getRetweetedStatus().getUser().getId());
                builder.field("user_screen_name", status.getRetweetedStatus().getUser().getScreenName());
                if (status.getRetweetedStatus().getRetweetCount() != -1) {
                    builder.field("retweet_count", status.getRetweetedStatus().getRetweetCount());
                }
            }
            builder.endObject();
        }

        if (status.getInReplyToStatusId() != -1) {
            builder.startObject("in_reply");
            builder.field("status", status.getInReplyToStatusId());
            if (status.getInReplyToUserId() != -1) {
                builder.field("user_id", status.getInReplyToUserId());
                builder.field("user_screen_name", status.getInReplyToScreenName());
            }
            builder.endObject();
        }

        if (status.getHashtagEntities() != null) {
            builder.startArray("hashtag");
            for (HashtagEntity hashtag : status.getHashtagEntities()) {
                builder.startObject();
                builder.field("text", hashtag.getText());
                builder.field("start", hashtag.getStart());
                builder.field("end", hashtag.getEnd());
                builder.endObject();
            }
            builder.endArray();
        }
        if (status.getContributors() != null && status.getContributors().length > 0) {
            builder.array("contributor", status.getContributors());
        }
        if (status.getGeoLocation() != null) {
            if (geoAsArray) {
                builder.startArray("location");
                builder.value(status.getGeoLocation().getLongitude());
                builder.value(status.getGeoLocation().getLatitude());
                builder.endArray();
            } else {
                builder.startObject("location");
                builder.field("lat", status.getGeoLocation().getLatitude());
                builder.field("lon", status.getGeoLocation().getLongitude());
                builder.endObject();
            }
        }
        
        if (status.getURLEntities() != null) {
            builder.startArray("link");
            for (URLEntity url : status.getURLEntities()) {
                if (url != null) {
                    builder.startObject();
                    if (url.getURL() != null) {
                        builder.field("url", url.getURL());
                    }
                    if (url.getDisplayURL() != null) {
                        builder.field("display_url", url.getDisplayURL());
                    }
                    if (url.getExpandedURL() != null) {
                        builder.field("expand_url", url.getExpandedURL());
                    }
                    builder.field("start", url.getStart());
                    builder.field("end", url.getEnd());
                    builder.endObject();
                }
            }
            builder.endArray();
        }

        

        builder.endObject();

        builder.endObject();
        
        return builder;
	}
	
	private static String getEnvelopeFromPlace(
			GeoLocation[][] boundingBoxCoordinates) {
		if (boundingBoxCoordinates != null && boundingBoxCoordinates.length > 0) {
			GeoLocation[] geoLocations = boundingBoxCoordinates[0];
			double minx = geoLocations[0].getLongitude();
			double miny = geoLocations[0].getLatitude();
			double maxy = geoLocations[1].getLatitude();
			double maxx = geoLocations[2].getLongitude();

			return  "[ [" +minx +", " +maxy +", [" +maxx +", " +miny +"] ]";
		}
		
		return null;
	}
	 
	 private static String generateGeoPointFromPlace(Status status){
     	GeoLocation[][] boundingBoxCoordinates = status.getPlace().getBoundingBoxCoordinates();
 		if(boundingBoxCoordinates != null && boundingBoxCoordinates.length > 0){
 			GeoLocation[] geoLocations = boundingBoxCoordinates[0];
 			double minx = geoLocations[0].getLongitude();
 			double miny = geoLocations[0].getLatitude();
 			double maxy = geoLocations[1].getLatitude();
 			double maxx = geoLocations[2].getLongitude();
 			
 			double x = minx + Math.random() * (maxx - minx);
 			double y = miny + Math.random() * (maxy - miny);
 			return y +"," + x;
 		}
 		return null;
     }
}
