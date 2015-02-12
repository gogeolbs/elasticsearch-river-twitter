package org.elasticsearch.river.twitter.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.twitter.connection.TwitterConnectionControl;

import twitter4j.FilterQuery;
import twitter4j.PagableResponseList;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.Configuration;

public class TwitterFiltering {
	private FilterQuery filterQuery;
	private Map<String, Object> filterSettings;
	private TwitterConnectionControl conn;

	public TwitterFiltering(Map<String, Object> filterSettings, TwitterConnectionControl conn) {
		filterQuery = new FilterQuery();
        filterQuery.count(XContentMapValues.nodeIntegerValue(filterSettings.get("count"), 0));
		this.filterSettings = filterSettings;
		this.conn = conn;
	}
	
	public FilterQuery getFilterQuery() {
		return filterQuery;
	}

	@SuppressWarnings("unchecked")
	public void filterTrack(Object tracks){
		if (tracks instanceof List) {
			List<String> lTracks = (List<String>) tracks;
			filterQuery.track(lTracks.toArray(new String[lTracks.size()]));
		} else {
			filterQuery.track(Strings
					.commaDelimitedListToStringArray(tracks.toString()));
		}
	}
	
	public void filterFollow(Object follow){
		if (follow instanceof List) {
			List<?> lFollow = (List<?>) follow;
			long[] followIds = new long[lFollow.size()];
			for (int i = 0; i < lFollow.size(); i++) {
				Object o = lFollow.get(i);
				if (o instanceof Number) {
					followIds[i] = ((Number) o).intValue();
				} else {
					followIds[i] = Long.parseLong(o.toString());
				}
			}
			filterQuery.follow(followIds);
		} else {
			String[] ids = Strings.commaDelimitedListToStringArray(follow
					.toString());
			long[] followIds = new long[ids.length];
			for (int i = 0; i < ids.length; i++) {
				followIds[i] = Long.parseLong(ids[i]);
			}
			filterQuery.follow(followIds);
		}
	}
	
	public void filterLocation(Object locations){
		if (locations instanceof List) {
			List<?> lLocations = (List<?>) locations;
			double[][] dLocations = new double[lLocations.size()][];
			for (int i = 0; i < lLocations.size(); i++) {
				Object loc = lLocations.get(i);
				double lat;
				double lon;
				if (loc instanceof List) {
					List<?> lLoc = (List<?>) loc;
					if (lLoc.get(0) instanceof Number) {
						lon = ((Number) lLoc.get(0)).doubleValue();
					} else {
						lon = Double.parseDouble(lLoc.get(0).toString());
					}
					if (lLoc.get(1) instanceof Number) {
						lat = ((Number) lLoc.get(1)).doubleValue();
					} else {
						lat = Double.parseDouble(lLoc.get(1).toString());
					}
				} else {
					String[] sLoc = Strings
							.commaDelimitedListToStringArray(loc.toString());
					lon = Double.parseDouble(sLoc[0]);
					lat = Double.parseDouble(sLoc[1]);
				}
				dLocations[i] = new double[] { lon, lat };
			}
			filterQuery.locations(dLocations);
		} else {
			String[] sLocations = Strings
					.commaDelimitedListToStringArray(locations.toString());
			double[][] dLocations = new double[sLocations.length / 2][];
			int dCounter = 0;
			for (int i = 0; i < sLocations.length; i++) {
				double lon = Double.parseDouble(sLocations[i]);
				double lat = Double.parseDouble(sLocations[++i]);
				dLocations[dCounter++] = new double[] { lon, lat };
			}
			filterQuery.locations(dLocations);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void filterUserLists(Object userLists){
		if (userLists instanceof List) {
			List<String> lUserlists = (List<String>) userLists;
			String[] tUserlists = lUserlists.toArray(new String[lUserlists
					.size()]);
			filterQuery.follow(getUsersListMembers(tUserlists));
		} else {
			String[] tUserlists = Strings
					.commaDelimitedListToStringArray(userLists.toString());
			filterQuery.follow(getUsersListMembers(tUserlists));
		}
	}
	
	@SuppressWarnings("unchecked")
	public void filterLanguage(Object language){
		if (language instanceof List) {
			List<String> lLanguage = (List<String>) language;
			filterQuery.language(lLanguage.toArray(new String[lLanguage
					.size()]));
		} else {
			filterQuery.language(Strings
					.commaDelimitedListToStringArray(language.toString()));
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public void filter() {
		filterQuery.count(XContentMapValues.nodeIntegerValue(
				filterSettings.get("count"), 0));
		Object tracks = filterSettings.get("tracks");
		boolean filterSet = false;
		if (tracks != null) {
			if (tracks instanceof List) {
				List<String> lTracks = (List<String>) tracks;
				filterQuery.track(lTracks.toArray(new String[lTracks.size()]));
			} else {
				filterQuery.track(Strings
						.commaDelimitedListToStringArray(tracks.toString()));
			}
			filterSet = true;
		}
		Object follow = filterSettings.get("follow");
		if (follow != null) {
			if (follow instanceof List) {
				List<?> lFollow = (List<?>) follow;
				long[] followIds = new long[lFollow.size()];
				for (int i = 0; i < lFollow.size(); i++) {
					Object o = lFollow.get(i);
					if (o instanceof Number) {
						followIds[i] = ((Number) o).intValue();
					} else {
						followIds[i] = Long.parseLong(o.toString());
					}
				}
				filterQuery.follow(followIds);
			} else {
				String[] ids = Strings.commaDelimitedListToStringArray(follow
						.toString());
				long[] followIds = new long[ids.length];
				for (int i = 0; i < ids.length; i++) {
					followIds[i] = Long.parseLong(ids[i]);
				}
				filterQuery.follow(followIds);
			}
			filterSet = true;
		}
		Object locations = filterSettings.get("locations");
		if (locations != null) {
			if (locations instanceof List) {
				List<?> lLocations = (List<?>) locations;
				double[][] dLocations = new double[lLocations.size()][];
				for (int i = 0; i < lLocations.size(); i++) {
					Object loc = lLocations.get(i);
					double lat;
					double lon;
					if (loc instanceof List) {
						List<?> lLoc = (List<?>) loc;
						if (lLoc.get(0) instanceof Number) {
							lon = ((Number) lLoc.get(0)).doubleValue();
						} else {
							lon = Double.parseDouble(lLoc.get(0).toString());
						}
						if (lLoc.get(1) instanceof Number) {
							lat = ((Number) lLoc.get(1)).doubleValue();
						} else {
							lat = Double.parseDouble(lLoc.get(1).toString());
						}
					} else {
						String[] sLoc = Strings
								.commaDelimitedListToStringArray(loc.toString());
						lon = Double.parseDouble(sLoc[0]);
						lat = Double.parseDouble(sLoc[1]);
					}
					dLocations[i] = new double[] { lon, lat };
				}
				filterQuery.locations(dLocations);
			} else {
				String[] sLocations = Strings
						.commaDelimitedListToStringArray(locations.toString());
				double[][] dLocations = new double[sLocations.length / 2][];
				int dCounter = 0;
				for (int i = 0; i < sLocations.length; i++) {
					double lon = Double.parseDouble(sLocations[i]);
					double lat = Double.parseDouble(sLocations[++i]);
					dLocations[dCounter++] = new double[] { lon, lat };
				}
				filterQuery.locations(dLocations);
			}
			filterSet = true;
		}
		Object userLists = filterSettings.get("user_lists");
		if (userLists != null) {
			if (userLists instanceof List) {
				List<String> lUserlists = (List<String>) userLists;
				String[] tUserlists = lUserlists.toArray(new String[lUserlists
						.size()]);
				filterQuery.follow(getUsersListMembers(tUserlists));
			} else {
				String[] tUserlists = Strings
						.commaDelimitedListToStringArray(userLists.toString());
				filterQuery.follow(getUsersListMembers(tUserlists));
			}
			filterSet = true;
		}

		// We should have something to filter
		if (!filterSet) {
			System.exit(0);
		}

		Object language = filterSettings.get("language");
		if (language != null) {
			if (language instanceof List) {
				List<String> lLanguage = (List<String>) language;
				filterQuery.language(lLanguage.toArray(new String[lLanguage
						.size()]));
			} else {
				filterQuery.language(Strings
						.commaDelimitedListToStringArray(language.toString()));
			}
		}
	}

	/**
	 * Get users id of each list to stream them.
	 * 
	 * @param tUserlists
	 *            List of user list. Should be a public list.
	 * @return
	 */
	private long[] getUsersListMembers(String[] tUserlists) {
		List<Long> listUserIdToFollow = new ArrayList<Long>();
		Configuration cb = conn.buildTwitterConfiguration();
		Twitter twitterImpl = new TwitterFactory(cb).getInstance();

		// For each list given in parameter
		for (String listId : tUserlists) {
			String[] splitListId = listId.split("/");
			try {
				long cursor = -1;
				PagableResponseList<User> itUserListMembers;
				do {
					itUserListMembers = twitterImpl.getUserListMembers(
							splitListId[0], splitListId[1], cursor);
					for (User member : itUserListMembers) {
						long userId = member.getId();
						listUserIdToFollow.add(userId);
					}
				} while ((cursor = itUserListMembers.getNextCursor()) != 0);

			} catch (TwitterException te) {
				te.printStackTrace();
			}
		}

		// Just casting from Long to long
		long ret[] = new long[listUserIdToFollow.size()];
		int pos = 0;
		for (Long userId : listUserIdToFollow) {
			ret[pos] = userId;
			pos++;
		}
		return ret;
	}
}
