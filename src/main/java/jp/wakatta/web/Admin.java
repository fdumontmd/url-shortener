package jp.wakatta.web;

import javax.servlet.ServletContext;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import jp.wakatta.client.JedisClient;

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupDir;

import redis.clients.jedis.JedisCommands;

@Path("/admin")
public class Admin {
	private static final int ONE_DAY = 3600 * 24;
	
	private JedisCommands client;
	
	public void setJedis(JedisClient jedis) {
		this.client = jedis.getClient();
	}

	@GET
	@Produces(MediaType.TEXT_HTML)
	public String getPage(@Context ServletContext sc) {
		return renderPage(sc, "", "", false, false);
	}

	protected String renderPage(ServletContext sc, String url, String shorter, boolean done, boolean error) {
		String templatePath = sc.getRealPath("/WEB-INF");
		STGroup group = new STGroupDir(templatePath + "/templates", '$', '$');
		ST page = group.getInstanceOf("index");
		page.add("url", url);
		page.add("shorter", shorter);
		page.add("done", done);
		page.add("error", error);
		
		return page.render();
	}
	
	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public String setUrl(
			@FormParam("url") String url,
			@FormParam("shorter") String shorter,
			@Context ServletContext sc
			) {
		if (client.exists(shorter)) {
			return "ERROR: key[" + shorter +"] already exists"; 
		} else {
			client.set(shorter, url);
			return "Key[" + shorter + "] mapped to URL[" + url + "]";
		}
	}
	
	@POST
	@Produces(MediaType.TEXT_HTML)
	public String setUrlHTML(
			@FormParam("url") String url,
			@FormParam("shorter") String shorter,
			@Context ServletContext sc
			) {
		
		boolean error = false;
		if (client.exists(shorter)) {
			error = true;
		} else {
			client.setex(shorter, ONE_DAY, url);
		}
		
		return renderPage(sc, url, shorter, !error, error);
	}
	
}
