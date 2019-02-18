package com.b3ds.ifarm.chatbot.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.b3ds.ifarm.chatbot.spark.models.ActiveSessions;
import com.b3ds.ifarm.chatbot.spark.models.Chat;
import com.b3ds.ifarm.chatbot.spark.models.Conversation;
import com.b3ds.ifarm.chatbot.spark.models.Sessions;
import com.b3ds.ifarm.chatbot.spark.udfs.UpdateLastActive;
import com.b3ds.ifarm.chatbot.spark.udfs.ValidateSession;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SessionManagement {

	private static String HDFS_URL ;
	public static Dataset<Row> activeSession = null;
	private static Dataset<Row> sessions = null;
	private static Dataset<Row> conversations = null;
	private static String hdfsuser;
	private static List<ActiveSessions> listActiveSessions = null;
	private static List<Sessions> listSessions = null;
	private static List<Conversation> listConversations = null;
	public static JsonArray conv = null;
	
	private SQLContext sql = null;
	private static SparkSession spark;
	private static Gson gson = null;
	
	private static Path conversationPath = null;
	private static FileSystem fs = null;
	
	public void test() throws IOException
	{
		List<Conversation> els = null;
		Type type = new TypeToken<List<Conversation>>() {
		}.getType();
		els = gson.fromJson(gson.toJson(conv), type);		
		String content = gson.toJson(conv);
		
	}
	
	
	public SessionManagement(final SparkSession session, String HDFS_URL, String hdfsuser) throws IOException
	{
		this.HDFS_URL = HDFS_URL;
		this.hdfsuser = hdfsuser;
		this.spark = session;
		gson = new Gson();		

		this.listConversations = new ArrayList<>();
		this.conv = gson.fromJson(gson.toJson(listConversations), JsonArray.class);
		
		this.listActiveSessions = new ArrayList<>();
		this.activeSession = activeSession(session);
		
		this.listSessions = new ArrayList<Sessions>();
		this.sessions = session.createDataFrame(listSessions, Sessions.class);
		
		this.sql = session.sqlContext();
		configureHdfs();
	}
	
	private void configureHdfs() throws IOException
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFS_URL);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		System.setProperty("HADOOP_USER_NAME", hdfsuser);
		System.setProperty("hadoop.home.dir", "/");
		fs = FileSystem.get(URI.create(HDFS_URL+"/demowrite"),conf);
		Path workingDir = fs.getWorkingDirectory();
		conversationPath = new Path("/home/chatbot/sessionmanagement/conversations");
		if(!fs.exists(conversationPath)) {
			   fs.mkdirs(conversationPath);
			}
		
	}
	
	private Dataset<Row> activeSession(SparkSession session)
	{
		return session.createDataFrame(listActiveSessions, ActiveSessions.class);
	}

	private boolean isValidSession(String userId)
	{
		Dataset<Row> df = activeSession.select(activeSession.col("*")).where(activeSession.col("userId").equalTo(userId));
		if (df.count() != 0){
			Long lastActive = df.select(df.col("lastActive")).collectAsList().get(0).getAs(0);
			String sessionId = df.select(df.col("sessionId")).collectAsList().get(0).getAs(0);
						
			if ((System.currentTimeMillis() - lastActive) >= 1*60*1000)
			{
				System.out.println("session is expired");
				saveSession(df);
				storeExpiredSessionConversation(sessionId);
				df.unpersist();
				this.activeSession = activeSession.filter("userId!="+userId);
				return false;
			}
		}
		return true;
	}
	
	private void saveSession(Dataset<Row> df)
	{
		List<String> ll = df.toJSON().collectAsList();
		ActiveSessions ac = gson.fromJson(ll.get(0), ActiveSessions.class);
		listActiveSessions.remove(ac);
		
		Sessions s = gson.fromJson(ll.get(0), Sessions.class);
		s.setEndedAt(new Timestamp(new Date().getTime()).toString());
		s.setActive(false);
		List<Sessions> listSessions = new ArrayList();
		listSessions.add(s);
		Dataset<Row> dd = spark.createDataFrame(listSessions, Sessions.class);
		dd.write().
		mode(SaveMode.Append).
		json(HDFS_URL+"/home/chatbot/sessionmanagement/session");
				
		dd.unpersist();
	}

	private void storeExpiredSessionConversation(String sessionId)
	{
		for (JsonElement el : conv)
		{
			JsonObject obj = (JsonObject)el;
			
			if(obj.get("sessionId").getAsString().equals(sessionId))
			{
				String convs = gson.toJson(obj);
				try {
					writeConversationToHDFS(sessionId, convs);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void writeConversationToHDFS(String filename, String conversation) throws IOException
	{

		Path hdfswritepath = new Path(conversationPath + "/" + filename+".json");		
		FSDataOutputStream fd = fs.create(hdfswritepath);
		
		fd.writeBytes(conversation);
		fd.close();		
	}
		
	private String getCurrentSession(String userId, String source)
	{
		System.out.println("getCurrentSession");
		Dataset<Row> temp = activeSession.select(activeSession.col("*")).where(activeSession.col("UserId").equalTo(userId));
		String sessionId = null;
		if (temp.count() == 0)
		{
			sessionId = UUID.randomUUID().toString();
			createNewSession(sessionId, userId, source);
			return sessionId;
		}
		else if(isValidSession(userId)==false) {
			sessionId = UUID.randomUUID().toString();
			createNewSession(sessionId, userId, source);
			return sessionId;
		}
		else{
			return temp.select(temp.col("SessionId")).collectAsList().get(0).get(0).toString();
		}
	}
	
	private void createNewSession(String sessionId, String userId, String source)
	{
		ActiveSessions active = new ActiveSessions(sessionId, userId, new Timestamp(new Date().getTime()).toString()
				, true, source, System.currentTimeMillis(), null);
		this.listActiveSessions.add(active);
		this.activeSession = this.spark.createDataFrame(listActiveSessions, ActiveSessions.class);
	}
	
	public boolean saveConversation(String user_statement, String bot_statement, String userId, String type, String source)
	{
		boolean status = isValidSession(userId);
		if(status ==false)
		{
			return false;
		}
		String sessionId = getCurrentSession(userId, source);
		Conversation con = getConversation(sessionId);

		if (con == null)
		{
			List<Chat> statements = new ArrayList<>();
			Chat chat = new Chat(user_statement, bot_statement);
			statements.add(chat);
			Conversation conversation = new Conversation(sessionId, statements);
			conv.add(gson.fromJson(gson.toJson(conversation), JsonObject.class));
		}else {
			List<Chat> statements = con.getConversations();
			Chat chat = new Chat(user_statement, bot_statement);
			statements.add(chat);
			con.setConversations(statements);
			updateConversation(con);
		}
		updateActiveSession(sessionId);
		return true;
	}
	
	private void updateActiveSession(String sessionId)
	{
		Dataset<Row> temp = activeSession.select(activeSession.col("*")).where(activeSession.col("sessionId").equalTo(sessionId));
		temp =	temp.withColumn("lastActive", functions.callUDF("updateLastActive", functions.col("lastActive").cast(DataTypes.StringType)));
		temp = temp.cache();
		this.activeSession = activeSession.filter("sessionId != '"+sessionId+"'");
		this.activeSession = activeSession.union(temp);
		this.activeSession.show();
	}
	
	private Conversation getConversation(String sessionId )
	{
		for(JsonElement el : conv)
		{
			JsonObject obj = (JsonObject)el;
			if(obj.get("sessionId").getAsString().equals(sessionId))
			{
				Conversation con = gson.fromJson(obj, Conversation.class);
				return con;
			}
		}
		return null;
	}

	private void updateConversation(Conversation conversation)
	{
		for(JsonElement el : conv)
		{
			String sessionId = ((JsonObject)el).get("sessionId").getAsString();
			if(sessionId.equals(conversation.getSessionId()))
			{
				conv.remove(el);
				conv.add(gson.fromJson(gson.toJson(conversation), JsonObject.class));
				return ;
			}
		}
	}
	
/*	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
		SparkSession session = SparkSession.
				builder().
				config(conf).
				getOrCreate();
		session.udf().register("validate", new ValidateSession(), DataTypes.StringType);
		session.udf().register("updateLastActive", new UpdateLastActive(), DataTypes.LongType);
		
		SessionManagement mg = new SessionManagement(session);
		mg.saveConversation("hello", "Hi dear", "365", "Testing", "Testing");
		mg.saveConversation("wow", "what wow", "365", "Testing", "Testing");
		mg.test();
	}*/
}
