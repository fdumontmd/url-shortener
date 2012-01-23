package jp.wakatta.web;

import java.net.URI;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import jp.wakatta.client.JedisClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;

@Path("/s")
public class Client {
	private final static Logger LOGGER = LoggerFactory.getLogger(Client.class);

	private JedisCommands client;
	
	public void setJedis(JedisClient jedis) {
		this.client = jedis.getClient();
	}

	@GET
	@Path("/{shorter}")
	public Response getUrl(@PathParam("shorter") String shorter) throws Exception {
		LOGGER.info("getURL: [" + shorter + "]");
		String url = client.get(shorter);
		
		LOGGER.info("getURL: [" + url + "]");
		
		if (url == null) {
			return Response.status(Response.Status.NOT_FOUND).build();
		}
		return Response.seeOther(new URI(url)).build();
	}
}
