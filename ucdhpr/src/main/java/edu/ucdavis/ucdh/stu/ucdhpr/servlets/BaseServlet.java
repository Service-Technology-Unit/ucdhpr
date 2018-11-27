package edu.ucdavis.ucdh.stu.ucdhpr.servlets;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.itoc.snutil.beans.Event;
import edu.ucdavis.ucdh.itoc.snutil.util.EventService;

/**
 * <p>This is the base class for all servlets.</p>
 */
public abstract class BaseServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	protected String characterEncoding = "UTF-8";
	protected String contentType = "text/javascript;charset=UTF-8";
	protected String servletPath = "/";
	protected Log log = LogFactory.getLog(getClass());
	protected EventService eventService = null;
	protected String serviceNowServer = null;
	protected String serviceNowUser = null;
	protected String serviceNowPassword = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		eventService = (EventService) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("eventService");
		serviceNowServer = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowServer");
		serviceNowUser = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowUser");
		serviceNowPassword = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowPassword");
	}

	/**
	 * <p>Returns the id present in the URL, if any.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the id present in the URL, if any
	 */
	protected String getIdFromUrl(HttpServletRequest req) {
		String id = null;

		String basePath = req.getContextPath() + servletPath;
		if (req.getRequestURI().length() > basePath.length()) {
			id = req.getRequestURI().substring(basePath.length());
		}

		return id;
	}

	/**
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, JSONObject details) throws IOException {
		sendError(req, res, errorCode, errorMessage, details, null);
	}

	/**
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 * @param throwable an optional exception
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, JSONObject details, Throwable throwable) throws IOException {
		// log message
		if (throwable != null) {
			log.error("Sending error " + errorCode + "; message=" + errorMessage, throwable);
		} else if (log.isDebugEnabled()) {
			log.debug("Sending error " + errorCode + "; message=" + errorMessage);
		}

		// verify details
		if (details == null) {
			details = new JSONObject();
		}

		// log event
		eventService.logEvent(new Event((String) details.get("id"), "HTTP response", "Sending error " + errorCode + "; message=" + errorMessage, details, throwable));

		// send error
		res.setContentType("text/plain;charset=UTF-8");
		res.sendError(errorCode, errorMessage);
	}
}
