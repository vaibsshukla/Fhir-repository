package com.b3ds.ifarm.chatbot.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class ValidateSession implements UDF1<String, String>{

	private final String validate(final String text)
	{
		return "Hello";
	}
	
	@Override
	public String call(final String text) throws Exception {
		return validate(text);
	}

}
