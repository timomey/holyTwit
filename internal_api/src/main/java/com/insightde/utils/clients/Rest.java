package com.insightde.utils.clients;

import static org.apache.log4j.Logger.getLogger;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;


public class Rest {
	private static final Logger logger = getLogger(Rest.class);
	
	private static final String mediaType =  "application/json";
	
	
	public static String postForm(String url, Form form){
		ResteasyClient client = new ResteasyClientBuilder().build();
		ResteasyWebTarget target = client.target(url);
		Entity<Form> entity = Entity.form(form);
	    Response response = target.request(MediaType.APPLICATION_JSON).post(entity);
	    return response.readEntity(String.class);
	}
	
	@SuppressWarnings("deprecation")
	public static String post(String url,String request){
		try{
			ClientRequest clientRequest = new ClientRequest(url).accept(mediaType).body(mediaType, request);
//			clientRequest.header("Content-Type", "application/json");
			ClientResponse<String> clientResponse = clientRequest.post(String.class);
			return clientResponse.getEntity();
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		return null;
	}
}
