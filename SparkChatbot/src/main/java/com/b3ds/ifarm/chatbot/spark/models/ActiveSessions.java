package com.b3ds.ifarm.chatbot.spark.models;

import com.google.gson.annotations.SerializedName;

public class ActiveSessions {
	
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

	@SerializedName("lastActive")
	private Long LastActive;
	
	@SerializedName("session_type")
	private String SessionType;

	public String getSessionId() {
		return SessionId;
	}

	public void setSessionId(String sessionId) {
		SessionId = sessionId;
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

	public Long getLastActive() {
		return LastActive;
	}

	public void setLastActive(Long lastActive) {
		LastActive = lastActive;
	}

	public String getSessionType() {
		return SessionType;
	}

	public void setSessionType(String sessionType) {
		SessionType = sessionType;
	}

	public String getUserId() {
		return UserId;
	}

	public void setUserId(String userId) {
		UserId = userId;
	}

	public ActiveSessions(String sessionId, String userId, String startedAt, boolean active, String source,
			Long lastActive, String sessionType) {
		super();
		SessionId = sessionId;
		UserId = userId;
		StartedAt = startedAt;
		Active = active;
		Source = source;
		LastActive = lastActive;
		SessionType = sessionType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((SessionId == null) ? 0 : SessionId.hashCode());
		result = prime * result + ((StartedAt == null) ? 0 : StartedAt.hashCode());
		result = prime * result + ((UserId == null) ? 0 : UserId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ActiveSessions other = (ActiveSessions) obj;
		if (SessionId == null) {
			if (other.SessionId != null)
				return false;
		} else if (!SessionId.equals(other.SessionId))
			return false;
		if (StartedAt == null) {
			if (other.StartedAt != null)
				return false;
		} else if (!StartedAt.equals(other.StartedAt))
			return false;
		if (UserId == null) {
			if (other.UserId != null)
				return false;
		} else if (!UserId.equals(other.UserId))
			return false;
		return true;
	}
		
	
}
