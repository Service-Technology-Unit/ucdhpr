package edu.ucdavis.ucdh.stu.ucdhpr.servlets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

/**
 * <p>This servlet publishes changes from the IAM Person Repository.</p>
 */
public class PersonUpdateServlet extends BaseServlet {
	private static final long serialVersionUID = 1;
	private List<Map<String, String>> field = new ArrayList<Map<String, String>>();
	private String publisherId = null;
	private Up2DateService up2dateService = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		publisherId = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("iamPublisherId");
		up2dateService = (Up2DateService) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("up2dateService");
	}

	/**
	 * <p>The Servlet "GET" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The GET method is not allowed for this URL", null);
    }

	/**
	 * <p>The Servlet "doPost" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		JSONObject response = new JSONObject();

		String id = req.getParameter("id");
		String action = req.getParameter("action");

		if (log.isDebugEnabled()) {
			log.debug("Processing new request - ID: " + id + "; Action: " + action);
		}

		JSONObject details = new JSONObject();
		details.put("id", id);
		details.put("action", action);
		details.put("response", response);

		if (field.size() == 0) {
			loadFields();
		}
		if (field.size() > 0) {
			if (StringUtils.isNotEmpty(id)) {
				if (StringUtils.isNotEmpty(action)) {
					if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("change") || action.equalsIgnoreCase("delete")) {
						processRequest(req, action, details);
					} else {
						response.put("responseCode", "2");
						response.put("response", "Error - Invalid action: " + action);
					}
				} else {
					response.put("responseCode", "2");
					response.put("response", "Error - Required parameter \"action\" has no value");
				}
			} else {
				response.put("responseCode", "2");
				response.put("response", "Error - Required parameter \"id\" has no value");
			}
		} else {
			response.put("responseCode", "3");
			response.put("response", "Internal configuration error; unable to process request");
		}

		if (log.isDebugEnabled()) {
			log.debug("Response: " + response);
		}

		if ("0".equals(response.get("responseCode"))) {
			res.setCharacterEncoding(characterEncoding);
			res.setContentType(contentType);
			res.getWriter().write(response.toJSONString());
		} else {
			if ("2".equals(response.get("responseCode"))) {
				sendError(req, res, HttpServletResponse.SC_BAD_REQUEST, response.toJSONString(), details);
			} else {
				sendError(req, res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.toJSONString(), details);
			}
		}
	}

	/**
	 * <p>Loads the fields from the publisher information.</p>
	 */
	private void loadFields() {
		Properties publisher = up2dateService.getPublisher(publisherId);
		if (publisher == null) {
			log.error("There is no publisher on file with an ID of " + publisherId + ".");
		} else {
			String[] fieldList = publisher.getProperty("field").split(";");
			for (int i=0; i<fieldList.length; i++) {
				HashMap<String, String> thisField = new HashMap<String, String>();
				String[] parts = fieldList[i].split(",");
				thisField.put("fieldName", parts[0]);
				thisField.put("columnName", parts[1]);
				field.add(thisField);
			}
			log.info("Publisher data obtained for publisher " + publisherId + "; data fields: " + field);
		}
	}

	/**
	 * <p>Processes the incoming transaction.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param action the requested action
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 */
	@SuppressWarnings({ "unchecked" })
	private void processRequest(HttpServletRequest req, String action, JSONObject details) {
		if ("delete".equals(action) || validateRequest(req, details)) {
			Properties properties = new Properties();
			for (Map<String, String> thisField : field) {
				String fieldName = thisField.get("fieldName");
				String fieldValue = req.getParameter(fieldName);
				if (StringUtils.isNotEmpty(fieldValue)) {
					properties.setProperty(fieldName, fieldValue);
					if (log.isDebugEnabled()) {
						log.debug("Field: " + fieldName + "; Value: " + fieldValue);
					}
				}
			}
			if (StringUtils.isEmpty(properties.getProperty("isEmployee"))) {
				if (StringUtils.isNotEmpty(properties.getProperty("adId"))) {
					properties.setProperty("isEmployee", "Y");
				} else {
					properties.setProperty("isEmployee", "N");
				}
			}
			if (StringUtils.isEmpty(properties.getProperty("isStudent"))) {
				if (StringUtils.isNotEmpty(properties.getProperty("studentId"))) {
					properties.setProperty("isStudent", "Y");
				} else {
					properties.setProperty("isStudent", "N");
				}
			}
			if (StringUtils.isEmpty(properties.getProperty("isExternal"))) {
				if (StringUtils.isNotEmpty(properties.getProperty("externalId"))) {
					properties.setProperty("isExternal", "Y");
				} else {
					properties.setProperty("isExternal", "N");
				}
			}
			up2dateService.post(new Update(publisherId, action, properties));
			JSONObject response = (JSONObject) details.get("response");
			response.put("responseCode", "0");
			response.put("response", "Transaction accepted.");
		}
	}

	/**
	 * <p>Processes the incoming request.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 */
	private boolean validateRequest(HttpServletRequest req, JSONObject details) {
		return true;
	}
}