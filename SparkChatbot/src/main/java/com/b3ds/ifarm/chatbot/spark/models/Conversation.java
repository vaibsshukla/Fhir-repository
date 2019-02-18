package com.b3ds.ifarm.chatbot.spark.models;

import java.io.Serializable;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Conversation implements Serializable {
	
	@SerializedName("sessionId")
	private String SessionId;
	
	@SerializedName("conversations")
	private List<Chat> Conversations;

	public String getSessionId() {
		return SessionId;
	}
	public void setSessionId(String sessionId) {
		SessionId = sessionId;
	}
	public List<Chat> getConversations() {
		return Conversations;
	}
	public void setConversations(List<Chat> conversations) {
		Conversations = conversations;
	}
	public Conversation(String sessionId, List<Chat> conversations) {
		super();
		SessionId = sessionId;
		Conversations = conversations;
	}
	
	
}
