package com.b3ds.ifarm.chatbot.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;

public class UpdateLastActive implements UDF1<String, Long>{
	
	
	private final Long update(final Long text)
	{
		System.out.println("udf called");
		return System.currentTimeMillis();
	}
	
	@Override
	public Long call(final String text) throws Exception {
		return update(Long.parseLong(text));
	}

}
