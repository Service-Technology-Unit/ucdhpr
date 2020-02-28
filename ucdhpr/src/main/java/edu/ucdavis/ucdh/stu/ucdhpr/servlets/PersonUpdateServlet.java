package edu.ucdavis.ucdh.stu.ucdhpr.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Enumeration;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class PersonUpdateServlet extends BaseServlet {
	private static final long serialVersionUID = 1L;
	private static final String INSERT_SQL = "INSERT INTO INCOMING (ACTION, MERGED_INTO, ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, MANAGER, DEPT_ID, IS_ACTIVE, IS_UCDH_EMPLOYEE, IS_UCD_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_UCDH_EMPLOYEE, IS_PREVIOUS_UCD_EMPLOYEE, IS_STUDENT, START_DATE, END_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, BANNER_ID, BANNER_START, BANNER_END, CAMPUS_PPS_ID, CAMPUS_PPS_START, CAMPUS_PPS_END, EXTERNAL_ID, EXTERNAL_START, EXTERNAL_END, UCDH_AD_ID, UCDH_AD_START, UCDH_AD_END, KERBEROS_ID, KERBEROS_START, KERBEROS_END, MOTHRA_ID, MOTHRA_START, MOTHRA_END, PPS_ID, PPS_START, PPS_END, STUDENT_ID, STUDENT_START, STUDENT_END, STUDENT_MAJOR, STUDENT_MAJOR_NAME, UC_PATH_ID, UC_PATH_INSTITUTION, UC_PATH_TYPE, UC_PATH_PERCENT, UC_PATH_REPRESENTATION, UC_PATH_START, UC_PATH_END, CREATED_ON, CREATED_BY, CREATED_FROM, UPDATE_CT, UPDATED_ON, UPDATED_BY, UPDATED_FROM) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, 0, getdate(), ?, ?)";
	private DataSource dataSource = null;

	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		this.dataSource = (DataSource)WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("masterDataSource");
	}

	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, 405, "The GET method is not allowed for this URL", (JSONObject)null);
	}

	@SuppressWarnings("unchecked")
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		JSONObject response = new JSONObject();
		String id = req.getParameter("id");
		String action = req.getParameter("action");
		if (this.log.isDebugEnabled()) {
			log.debug("Processing new request - ID: " + id + "; Action: " + action);
		}
		JSONObject details = new JSONObject();
		JSONObject request = new JSONObject();
		details.put("id", id);
		details.put("action", action);
		details.put("request", request);
		details.put("response", response);
		Enumeration<String> parameterNames = req.getParameterNames();
		while (parameterNames.hasMoreElements()) {
			String key = parameterNames.nextElement();
			request.put(key, req.getParameter(key));
			log.info(key + ": " + req.getParameter(key));
		}
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
		if (this.log.isDebugEnabled()) {
			log.debug("Response: " + response);
		}
		if ("0".equals(response.get("responseCode"))) {
			res.setCharacterEncoding(characterEncoding);
			res.setContentType(contentType);
			res.getWriter().write(response.toJSONString());
		} else if ("2".equals(response.get("responseCode"))) {
			sendError(req, res, 400, response.toJSONString(), details);
		} else {
			sendError(req, res, 500, response.toJSONString(), details);
		}
	}

	@SuppressWarnings("unchecked")
	private void processRequest(HttpServletRequest req, String action, JSONObject details) {
		JSONObject response = (JSONObject)details.get("response");
		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		if ("delete".equals(action) || validateRequest(req, details)) {
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = this.dataSource.getConnection();
				ps = conn.prepareStatement(INSERT_SQL);
				ps.setString(1, nullify(req.getParameter("action")));
				ps.setString(2, nullify(req.getParameter("mergedInto")));
				ps.setString(3, nullify(req.getParameter("id")));
				ps.setString(4, nullify(req.getParameter("lastName")));
				ps.setString(5, nullify(req.getParameter("firstName")));
				ps.setString(6, nullify(req.getParameter("middleName")));
				ps.setString(7, nullify(req.getParameter("title")));
				ps.setString(8, nullify(req.getParameter("supervisor")));
				ps.setString(9, nullify(req.getParameter("manager")));
				ps.setString(10, nullify(req.getParameter("deptId")));
				if ("delete".equalsIgnoreCase(req.getParameter("action"))) {
					ps.setString(11, "N");
				} else {
					ps.setString(11, "Y");
				} 
				if ("UCDH".equalsIgnoreCase(req.getParameter("ucPathInstitution")) && StringUtils.isNotEmpty(req.getParameter("ucPathId"))) {
					ps.setString(12, "Y");
				} else {
					ps.setString(12, "N");
				} 
				if ("UCD".equalsIgnoreCase(req.getParameter("ucPathInstitution")) && StringUtils.isNotEmpty(req.getParameter("ucPathId"))) {
					ps.setString(13, "Y");
				} else {
					ps.setString(13, "N");
				} 
				if (StringUtils.isNotEmpty(req.getParameter("externalId"))) {
					ps.setString(14, "Y");
				} else {
					ps.setString(14, "N");
				} 
				if ("UCDH".equalsIgnoreCase(req.getParameter("ucPathInstitution")) && StringUtils.isNotEmpty(req.getParameter("ucPathId")) && StringUtils.isNotEmpty(req.getParameter("ucPathEnd"))) {
					ps.setString(15, "Y");
				} else {
					ps.setString(15, "N");
				} 
				if ("UCD".equalsIgnoreCase(req.getParameter("ucPathInstitution")) && StringUtils.isNotEmpty(req.getParameter("ucPathId")) && StringUtils.isNotEmpty(req.getParameter("ucPathEnd"))) {
					ps.setString(16, "Y");
				} else {
					ps.setString(16, "N");
				} 
				if (StringUtils.isNotEmpty(req.getParameter("studentId"))) {
					ps.setString(17, "Y");
				} else {
					ps.setString(17, "N");
				} 
				ps.setString(18, nullify(req.getParameter("startDate")));
				ps.setString(19, nullify(req.getParameter("endDate")));
				ps.setString(20, nullify(req.getParameter("phoneNr")));
				ps.setString(21, nullify(req.getParameter("cellNr")));
				ps.setString(22, nullify(req.getParameter("pagerNr")));
				ps.setString(23, nullify(req.getParameter("pagerProvider")));
				ps.setString(24, nullify(req.getParameter("alternatePhones")));
				ps.setString(25, nullify(req.getParameter("email")));
				ps.setString(26, nullify(req.getParameter("alternateEmail")));
				ps.setString(27, nullify(req.getParameter("locationCode")));
				ps.setString(28, nullify(req.getParameter("bannerId")));
				ps.setString(29, nullify(req.getParameter("bannerStart")));
				ps.setString(30, nullify(req.getParameter("bannerEnd")));
				if ("UCD".equalsIgnoreCase(req.getParameter("ucPathInstitution")) && StringUtils.isNotEmpty(req.getParameter("ppsId"))) {
					ps.setString(31, nullify(req.getParameter("ppsId")));
					ps.setString(32, nullify(req.getParameter("ucPathStart")));
					ps.setString(33, nullify(req.getParameter("ucPathEnd")));
				} else {
					ps.setString(31, null);
					ps.setString(32, null);
					ps.setString(33, null);
				} 
				ps.setString(34, nullify(req.getParameter("externalId")));
				ps.setString(35, nullify(req.getParameter("externalStart")));
				ps.setString(36, nullify(req.getParameter("externalEnd")));
				ps.setString(37, nullify(req.getParameter("ucdhAdId")));
				ps.setString(38, nullify(req.getParameter("ucdhAdStart")));
				ps.setString(39, nullify(req.getParameter("ucdhAdEnd")));
				ps.setString(40, nullify(req.getParameter("kerberosId")));
				ps.setString(41, nullify(req.getParameter("kerberosStart")));
				ps.setString(42, nullify(req.getParameter("kerberosEnd")));
				ps.setString(43, nullify(req.getParameter("mothraId")));
				ps.setString(44, nullify(req.getParameter("mothraStart")));
				ps.setString(45, nullify(req.getParameter("mothraEnd")));
				if ("UCDH".equalsIgnoreCase(req.getParameter("ucPathInstitution")) && StringUtils.isNotEmpty(req.getParameter("ppsId"))) {
					ps.setString(46, nullify(req.getParameter("ppsId")));
					ps.setString(47, nullify(req.getParameter("ucPathStart")));
					ps.setString(48, nullify(req.getParameter("ucPathEnd")));
				} else {
					ps.setString(46, null);
					ps.setString(47, null);
					ps.setString(48, null);
				} 
				ps.setString(49, nullify(req.getParameter("studentId")));
				ps.setString(50, nullify(req.getParameter("studentStart")));
				ps.setString(51, nullify(req.getParameter("studentEnd")));
				ps.setString(52, nullify(req.getParameter("studentMajor")));
				ps.setString(53, nullify(req.getParameter("studentMajorName")));
				ps.setString(54, nullify(req.getParameter("ucPathId")));
				ps.setString(55, nullify(req.getParameter("ucPathInstitution")));
				ps.setString(56, nullify(req.getParameter("ucPathType")));
				ps.setString(57, nullify(req.getParameter("ucPathPercent")));
				ps.setString(58, nullify(req.getParameter("ucPathRepresentation")));
				ps.setString(59, nullify(req.getParameter("ucPathStart")));
				ps.setString(60, nullify(req.getParameter("ucPathEnd")));
				ps.setString(61, req.getRemoteUser());
				ps.setString(62, remoteAddr);
				ps.setString(63, req.getRemoteUser());
				ps.setString(64, remoteAddr);
				if (ps.executeUpdate() > 0) {
					response.put("responseCode", "0");
					response.put("response", "Transaction accepted");
				} else {
					response.put("responseCode", "3");
					response.put("response", "0 rows inserted into the target database");
					log.error("0 rows inserted into the target database");
					logEvent((String)details.get("id"), "IAM Transaction Error", "0 rows inserted into the target database", details);
				} 
			} catch (Exception e) {
				response.put("responseCode", "3");
				response.put("response", "Exception encountered attempting to save incoming data: " + e.toString());
				log.error("Exception encountered attempting to save incoming data: " + e.toString(), e);
				logEvent((String)details.get("id"), "IAM Transaction Error", "Exception encountered attempting to save incoming data: " + e.toString(), details, e);
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception exception) {
						
					}
				}
				if (conn != null) {
					try {
						conn.close();
					} catch (Exception exception) {
						
					}
				}
			} 
		} else {
			response.put("responseCode", "3");
			response.put("response", details.get("validationErrors"));
			log.error("Incoming data validate errors: " + details.get("validationErrors"));
			logEvent((String)details.get("id"), "IAM Transaction Error", "Incoming data validate errors: " + details.get("validationErrors"), details);
		} 
	}
	
	@SuppressWarnings("unchecked")
	private boolean validateRequest(HttpServletRequest req, JSONObject details) {
		boolean isValid = true;
		String validationErrors = "";
		details.put("validationErrors", validationErrors);
		return isValid;
	}
	
	private String nullify(String string) {
		String response = null;
		if (StringUtils.isNotEmpty(string) && !string.toLowerCase().equals("null") && !string.toLowerCase().equals("none") && !string.toLowerCase().equals("n/a")) {
			response = string.trim();
		}
		return response;
	}
}