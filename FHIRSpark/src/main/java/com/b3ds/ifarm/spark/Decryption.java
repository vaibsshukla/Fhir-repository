package com.b3ds.ifarm.spark;

import java.nio.ByteBuffer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.net.util.Base64;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;

public class Decryption implements UDF1<String, String>{

	private final static String PASSWORD = "skfjdkfjkak458475iwejdk$%&mksfmksmdf==";
	private ByteBuffer buffer = null;
	
	@SuppressWarnings({ "static-access", "rawtypes" })
	private String decrypt(final String encryptedText) throws Exception {

	    Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
	    
		buffer = ByteBuffer.wrap(new Base64().decode(encryptedText));
		
	    
	    final byte[] saltBytes = new byte[20];
	    buffer.get(saltBytes, 0, saltBytes.length);
	    
	    final byte[] ivBytes1 = new byte[cipher.getBlockSize()];
	    buffer.get(ivBytes1, 0, ivBytes1.length);
	    
	    final byte[] encryptedTextBytes = new byte[buffer.capacity() - saltBytes.length - ivBytes1.length];
	  
	    buffer.get(encryptedTextBytes);
	    
	    // Deriving the key
	    SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
	    PBEKeySpec spec = new PBEKeySpec(PASSWORD.toCharArray(), saltBytes, 65556, 256);
	    SecretKey secretKey = factory.generateSecret(spec);
	    SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");
	    cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(ivBytes1));
	    byte[] decryptedTextBytes = null;
	    try {
	      decryptedTextBytes = cipher.doFinal(encryptedTextBytes);
	    } catch (IllegalBlockSizeException e) {
	        e.printStackTrace();
	    } catch (BadPaddingException e) {
	        e.printStackTrace();
	    }
	   
	    return new String(decryptedTextBytes);
	  }

	@Override
	public String call(String encryptedText) throws Exception {
		return decrypt(encryptedText);
	}
	
}
