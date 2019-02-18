package com.b3ds.ifarm.chatbot.spark.models;

import com.google.gson.annotations.SerializedName;

public class Sessions {
	
	@SerializedName("sessionId")
	private String SessionId;

	@SerializedName("userId")
	private String UserId;

	@SerializedName("startedAt")
	private String StartedAt;

	@SerializedName("active")
	private boolean Active;

	@SerializedName("source")
	private String Source;

	@SerializedName("endedAt")
	private String EndedAt;
	
	@SerializedName("session_type")
	private String SessionType;

	public String getSessionId() {
		return SessionId;
	}

	public void setSessionId(String sessionId) {
		SessionId = sessionId;
	}

	public String getUserId() {
		return UserId;
	}

	public void setUserId(String userId) {
		UserId = userId;
	}

	public String getStartedAt() {
		return StartedAt;
	}

	public void setStartedAt(String startedAt) {
		StartedAt = startedAt;
	}

	public boolean isActive() {
		return Active;
	}

	public void setActive(boolean active) {
		Active = active;
	}

	public String getSource() {
		return Source;
	}

	public void setSource(String source) {
		Source = source;
	}

	public String getEndedAt() {
		return EndedAt;
	}

	public void setEndedAt(String endedAt) {
		EndedAt = endedAt;
	}

	public String getSessionType() {
		return SessionType;
	}

	public void setSessionType(String sessionType) {
		SessionType = sessionType;
	}

	public Sessions(String sessionId, String userId, String startedAt, boolean active, String source, String endedAt,
			String sessionType) {
		super();
		SessionId = sessionId;
		UserId = userId;
		StartedAt = startedAt;
		Active = active;
		Source = source;
		EndedAt = endedAt;
		SessionType = sessionType;
	}

	
}
