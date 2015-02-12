package org.elasticsearch.river.twitter.handlers;

import twitter4j.Status;
import twitter4j.StatusAdapter;
import twitter4j.StatusDeletionNotice;
import twitter4j.UserStreamAdapter;

public class UserStreamHandler extends UserStreamAdapter {

	private final StatusAdapter statusHandler;
	
	public UserStreamHandler(StatusAdapter statusHandler) {
		this.statusHandler = statusHandler;
	}

	@Override
	public void onException(Exception ex) {
		statusHandler.onException(ex);
	}

	@Override
	public void onStatus(Status status) {
		statusHandler.onStatus(status);
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		statusHandler.onDeletionNotice(statusDeletionNotice);
	}
}