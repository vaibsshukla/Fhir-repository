package com.b3ds.ifarm.chatbot.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.b3ds.ifarm.chatbot.spark.udfs.UpdateLastActive;
import com.b3ds.ifarm.chatbot.spark.udfs.ValidateSession;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import kafka.serializer.StringDecoder;


public class KafkaSpark{
	static Gson gson = new Gson();	
	private static String replyUrl; 
	private static HttpPost request; 
	
	private static SessionManagement SESSION_MANAGEMENT = null;
	private static final Logger logger = LogManager.getLogger(KafkaSpark.class);
	
	
	public static void main(String[] args) throws Exception {
		
		Logger.getLogger("org").setLevel(Level.OFF);
		logger.getLogger("com").setLevel(Level.INFO);
		
		checkArgs(args);
		String bootstrap = getArgs(args[0]);
		String chatbot = getArgs(args[1]);
		String messenger = getArgs(args[2]);
		String hdfsurl = getArgs(args[3]);
		String hdfsUser = getArgs(args[4]);
		
/*		String bootstrap = "localhost:9092";
		final String chatbot = "localhost:8000";
		String messenger = "localhost:8081";
		String hdfsurl = "hdfs://localhost:8020";
		String hdfsUser="b3ds";*/
		
		replyUrl = getArgs("http://"+messenger+"/kafka/publish/reply");
		request = new HttpPost(replyUrl);
		
		SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
		SparkSession session = SparkSession.
				builder().
				config(conf).
				getOrCreate();
		
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());
		JavaStreamingContext streamingContext = new JavaStreamingContext(sc, new Duration(1000));
		
        Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", bootstrap);
//		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("auto.offset.reset", "largest");
		Set<String> topics = Collections.singleton("send-message");
		
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(streamingContext, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		session.udf().registerJava("updateLastActive", UpdateLastActive.class.getName(), DataTypes.LongType);

		try {
			SESSION_MANAGEMENT = new SessionManagement(session, hdfsurl, hdfsUser);
		} catch (IOException e) {
			e.printStackTrace();
		}

		directKafkaStream.foreachRDD(rdd -> {
			rdd.foreach(record -> sendMessageToBot(record._2, chatbot));
        });
		
		streamingContext.start();
		streamingContext.awaitTermination();
	}
	
	private static void checkArgs(String args[]) throws Exception
	{
		if(args.length < 4)
		{
			System.out.println("Usage :- java -jar --kafka=kafka-bootstrap-server --cb=chatterbot-host:port --bm=Botmessenger_url --hdfs=hdfs-url --hdfs-user=hdfs user");
			System.out.println("Example :- ");
			System.out.println("Example :- java -jar --kafka=localhost:9092 --cb=localhost:8000 --bm=localhost:8081 --hdfs=hdfs://localhost:8020 --hdfs-user=demo");
			throw new Exception("Please provide all arguments", new Throwable("Missing required arguments"));
		}
	}
	
	private static String getArgs(String str)
	{
		return str.substring(str.indexOf("=")+1);
	}
	@SuppressWarnings("deprecation")
	public static DefaultHttpClient getThreadSafeClient() {
	    DefaultHttpClient client = new DefaultHttpClient();
	    ClientConnectionManager mgr = client.getConnectionManager();
	    HttpParams params = client.getParams();
	 
	    client = new DefaultHttpClient(new ThreadSafeClientConnManager(params, 
	            mgr.getSchemeRegistry()), params);	 
	    return client;
	}
	
	public static void sendMessageToBot(String s,String chatbot)
	{
		JsonObject obj = gson.fromJson(s, JsonObject.class);
		String url = "http://"+chatbot+"/get?msg="+obj.get("text").getAsString()
				+"&userId="+obj.get("userId").getAsLong()+"&userName="+obj.get("userName").getAsString();
		HttpGet request = new HttpGet(url);
		HttpResponse response = null;
		try {
			response = getThreadSafeClient().execute(request);
			InputStream io = response.getEntity().getContent();
			String res = IOUtils.toString(io);
			sendBotReplyToUser(res,s);
//			System.out.println("raw message = "+res);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void sendBotReplyToUser(String message, String user_message)
	{
		JsonObject obj = gson.fromJson(message, JsonObject.class);
		boolean status = SESSION_MANAGEMENT.saveConversation(user_message, obj.get("text").getAsString(), obj.get("chat_id").getAsString(), "Query", "Telegram");
		if(status == false)
		{
			obj.addProperty("text", "Your Session is Expired. You can start new session.");
		}
		try {
			StringEntity entity = new StringEntity(gson.toJson(obj), "UTF-8");
			entity.setContentType("application/json");
			request.setEntity(entity);
			HttpResponse response = getThreadSafeClient().execute(request);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException cpe) {
			cpe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	
}
