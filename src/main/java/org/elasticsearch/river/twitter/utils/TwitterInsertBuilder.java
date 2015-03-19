package org.elasticsearch.river.twitter.utils;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.MediaEntity.Size;
import twitter4j.Status;
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
        
        if(location == null)
        	return null;
        
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        
        //root informations
        builder.field("location", location);
        builder.field("created_at", status.getCreatedAt());
        builder.field("id", status.getId());
        builder.field("text", status.getText());
        builder.field("source", status.getSource());
        builder.field("truncated", status.isTruncated());
        
        if (status.getInReplyToStatusId() >= 0)
        	builder.field("in_reply_to_status_id", status.getInReplyToStatusId());
        if(status.getInReplyToUserId() >= 0)
        	builder.field("in_reply_to_user_id", status.getInReplyToUserId());
        builder.field("in_reply_to_screen_name", status.getInReplyToScreenName());
        
        if (status.getContributors() != null && status.getContributors().length > 0) {
            builder.array("contributors", status.getContributors());
        }
        if (status.getRetweetCount() >= 0) {
            builder.field("retweet_count", status.getRetweetCount());
        }
        
        if (status.getFavoriteCount() >= 0) {
            builder.field("favorite_count", status.getFavoriteCount());
        }
        
        builder.field("favorited", status.isFavorited());
        builder.field("retweeted", status.isRetweeted());
        builder.field("possibly_sensitive", status.isPossiblySensitive());
        builder.field("lang", status.getLang());
        builder.field("timestamp_ms", System.currentTimeMillis());

        if(status.getRetweetedStatus() != null) {
	        //retweet
	        builder.startObject("retweet");
	        builder.field("id", status.getRetweetedStatus().getId());
	        builder.endObject();
	        if(status.getRetweetedStatus().getRetweetCount() > 0)
	        	System.out.println(status.getRetweetedStatus().getRetweetCount());
        }
        
        //user
        builder.startObject("user");
        constructTwitterUser(status, builder);
        //end User
        builder.endObject();
        
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
            
            builder.startObject("attributes");
            builder.field("street_address", status.getPlace().getStreetAddress());
            builder.endObject();
            
            if(status.getPlace().getBoundingBoxCoordinates() != null && status.getPlace().getBoundingBoxCoordinates().length > 0) {
	            builder.startObject("bounding_box");
	            getEnvelopeFromPlace(status.getPlace().getBoundingBoxCoordinates(), builder);
	            builder.endObject();
            }
            
            builder.endObject();
        }
        
        //start Entities
        builder.startObject("entities");
        
        //Hashtags	
        if (status.getHashtagEntities() != null) {
            builder.startArray("hashtags");
            for (HashtagEntity hashtag : status.getHashtagEntities()) {
                builder.startObject();
                builder.field("text", hashtag.getText());
                
                if(hashtag.getStart() >= 0 && hashtag.getEnd() >= 0){
                	builder.array("indices", hashtag.getStart(), hashtag.getEnd());
                }
                builder.endObject();
            }
            builder.endArray();
        }
        
        //urls
        if (status.getURLEntities() != null) {
            builder.startArray("urls");
            for (URLEntity url : status.getURLEntities()) {
                if (url != null) {
                    builder.startObject();
                    if (url.getExpandedURL() != null) {
                        builder.field("expand_url", url.getExpandedURL());
                    }
                    
                    if(url.getStart() >= 0 && url.getEnd() >= 0){
                    	builder.array("indices", url.getStart(), url.getEnd());
                    }
                    
                    if (url.getDisplayURL() != null) {
                        builder.field("display_url", url.getDisplayURL());
                    }
                    if (url.getURL() != null) {
                        builder.field("url", url.getURL());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
        }
        
        //User mentions
        if (status.getUserMentionEntities() != null) {
            builder.startArray("user_mentions");
            for (UserMentionEntity mentions : status.getUserMentionEntities()) {
                builder.startObject();
                
                if(mentions.getId() >= 0)
                	builder.field("id", mentions.getId());
                
                builder.field("name", mentions.getName());
                
                if(mentions.getStart() >= 0 && mentions.getEnd() >= 0){
                	builder.array("indices", mentions.getStart(), mentions.getEnd());
                }
                
                builder.field("screen_name", mentions.getScreenName());
                builder.endObject();
            }
            builder.endArray();
        }
        
        //Symbols with $
        if (status.getSymbolEntities() != null) {
            builder.startArray("symbols");
            for (UserMentionEntity symbols : status.getUserMentionEntities()) {
                builder.startObject();
                builder.field("text", symbols.getText());
                
                if(symbols.getStart() >= 0 && symbols.getEnd() >= 0){
                	builder.array("indices", symbols.getStart(), symbols.getEnd());
                }
                
                builder.endObject();
            }
            builder.endArray();
        }
        
        //Media
        if (status.getMediaEntities() != null) {
            builder.startArray("media");
            for (MediaEntity media : status.getMediaEntities()) {
            	builder.startObject();
                builder.startObject("sizes");
                constructBuilderSize("thumb", media.getSizes().get(MediaEntity.Size.THUMB), builder);
                constructBuilderSize("small", media.getSizes().get(MediaEntity.Size.SMALL), builder);
                constructBuilderSize("medium", media.getSizes().get(MediaEntity.Size.MEDIUM), builder);
                constructBuilderSize("large", media.getSizes().get(MediaEntity.Size.LARGE), builder);
                builder.endObject();
                
                if(media.getId() >= 0)
                	builder.field("id", media.getId());
                builder.field("media_url_https", media.getMediaURLHttps());
                builder.field("media_url", media.getMediaURL());
                builder.field("expanded_url", media.getExpandedURL());
                
                if(media.getStart() >= 0 && media.getEnd() >= 0){
                	builder.array("indices", media.getStart(), media.getEnd());
                }
                
                
                builder.field("type", media.getType());
                builder.field("display_url", media.getDisplayURL());
                builder.field("url", media.getURL());
                builder.endObject();
            }
            
            builder.endArray();
        }
        
        //end entities
        builder.endObject();

        if(status.getScopes() != null && status.getScopes().getPlaceIds() != null) {
	        //scopes
	        builder.startArray("scopes");
	        for(String placeId :status.getScopes().getPlaceIds()){
	        	builder.startObject();
	        	builder.field("place_ids", placeId);
	        	builder.endObject();
	        }
	        builder.endArray();
        }
        
        if(status.getExtendedMediaEntities() != null){
	        //start Entities
	        builder.startArray("extended_entities");
	        for(MediaEntity media: status.getExtendedMediaEntities()){
	        	builder.startObject();
	            builder.startObject("sizes");
	            constructBuilderSize("thumb", media.getSizes().get(MediaEntity.Size.THUMB), builder);
	            constructBuilderSize("small", media.getSizes().get(MediaEntity.Size.SMALL), builder);
	            constructBuilderSize("medium", media.getSizes().get(MediaEntity.Size.MEDIUM), builder);
	            constructBuilderSize("large", media.getSizes().get(MediaEntity.Size.LARGE), builder);
	            builder.endObject();
	            
	            if(media.getId() >= 0)
	            	builder.field("id", media.getId());
	            builder.field("media_url_https", media.getMediaURLHttps());
	            builder.field("media_url", media.getMediaURL());
	            builder.field("expanded_url", media.getExpandedURL());
	            
	            if(media.getStart() >= 0 && media.getEnd() >= 0){
                	builder.array("indices", media.getStart(), media.getEnd());
                }
	            
	            builder.field("type", media.getType());
	            builder.field("display_url", media.getDisplayURL());
	            builder.field("url", media.getURL());
	            builder.endObject();
	        }
	        //end array extended entities
	        builder.endArray();
        }

        //end tweet
        builder.endObject();
        
        return builder;
	}
	
	private static void getEnvelopeFromPlace(
			GeoLocation[][] boundingBoxCoordinates, XContentBuilder builder) throws IOException {
		if (boundingBoxCoordinates != null && boundingBoxCoordinates.length > 0) {
			GeoLocation[] geoLocations = boundingBoxCoordinates[0];
			double minx = geoLocations[0].getLongitude();
			double miny = geoLocations[0].getLatitude();
			double maxy = geoLocations[1].getLatitude();
			double maxx = geoLocations[2].getLongitude();

	        builder.field("type", "envelope");
	        builder.startArray("coordinates");
	        toXContent(builder, minx, maxy);
	        toXContent(builder, maxx, miny);
	        builder.endArray();
		}
	}
	
	private static XContentBuilder toXContent(XContentBuilder builder, double x, double y) throws IOException {
        return builder.startArray().value(x).value(y).endArray();
    }
	 
	 private static String generateGeoPointFromPlace(Status status){
     	GeoLocation[][] boundingBoxCoordinates = status.getPlace().getBoundingBoxCoordinates();
 		if(boundingBoxCoordinates != null && boundingBoxCoordinates.length > 0){
 			GeoLocation[] geoLocations = boundingBoxCoordinates[0];
 			double minx = geoLocations[0].getLongitude();
 			double miny = geoLocations[0].getLatitude();
 			double maxy = geoLocations[1].getLatitude();
 			double maxx = geoLocations[2].getLongitude();
 			
// 			double x = minx + Math.random() * (maxx - minx);
// 			double y = miny + Math.random() * (maxy - miny);
 			double x_add = (maxx - minx) / 2;
 			double x = minx + x_add;
 			double y_add = (maxy - miny) / 2;
 			double y = miny + y_add;
 			if(y == 0 && x == 0)
 				return null;
 			return y +"," + x;
 		}
 		return null;
     }
	 
	 private static void constructBuilderSize(String type, Size size, XContentBuilder builder) throws IOException{
		 if(size == null)
			 return;
		 
		 builder.startObject(type);
		 builder.field("w", size.getWidth());
		 String resize = size.getResize() == MediaEntity.Size.FIT ? "fit" : "crop";
		 builder.field("resize", resize);
		 builder.field("h", size.getHeight());
		 builder.endObject();
		 
	 }
	 
	 private static void constructTwitterUser(Status status, XContentBuilder builder) throws IOException{
		builder.field("id", status.getUser().getId());
	    builder.field("name", status.getUser().getName());
	    builder.field("screen_name", status.getUser().getScreenName());
	    builder.field("location", status.getUser().getLocation());
	    builder.field("url", status.getUser().getURL());
	    builder.field("description", status.getUser().getDescription());
	    if (status.getUser().getFollowersCount() >= 0) {
	        builder.field("followers_count", status.getUser().getFollowersCount());
	    }
	    if (status.getUser().getFriendsCount() >= 0) {
	        builder.field("friends_count", status.getUser().getFriendsCount());
	    }
	    if (status.getUser().getListedCount() >= 0) {
	        builder.field("listed_count", status.getUser().getListedCount());
	    }
	    if (status.getUser().getFavouritesCount() >= 0) {
	        builder.field("favourites_count", status.getUser().getFavouritesCount());
	    }
	    if (status.getUser().getStatusesCount() >= 0) {
	        builder.field("statuses_count", status.getUser().getStatusesCount());
	    }
	    builder.field("created_at", status.getUser().getCreatedAt());
	    if (status.getUser().getUtcOffset() >= 0) {
	        builder.field("utc_offset", status.getUser().getUtcOffset());
	    }
	    builder.field("time_zone", status.getUser().getTimeZone());
	    builder.field("geo_enabled", status.getUser().isGeoEnabled());
	    builder.field("lang", status.getUser().getLang());
	    builder.field("profile_background_image_url", status.getUser().getProfileBackgroundImageURL());
	    builder.field("profile_image_url", status.getUser().getProfileImageURL());
	    builder.field("profile_banner_url", status.getUser().getProfileBannerURL());
	 }
}
