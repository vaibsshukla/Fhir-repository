package com.b3ds.ifarm.chatbot.spark.models;

import com.google.gson.annotations.SerializedName;

public class Chat {
	
	@SerializedName("user")
	private String User;

	@SerializedName("bot")
	private String Bot;
	
	public String getUser() {
		return User;
	}
	public void setUser(String user) {
		User = user;
	}
	public String getBot() {
		return Bot;
	}
	public void setBot(String bot) {
		Bot = bot;
	}
	public Chat(String user, String bot) {
		super();
		User = user;
		Bot = bot;
	}
	
	
}
