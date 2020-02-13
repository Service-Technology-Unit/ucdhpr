package edu.ucdavis.ucdh.stu.ucdhpr.batch;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

public class NewTransactionPost implements SpringBatchJob {
	private static final String SQL = "SELECT TRANSACTION_ID, ACTION, MERGED_INTO, ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, MANAGER, DEPT_ID, IS_ACTIVE, IS_UCDH_EMPLOYEE AS IS_EMPLOYEE, IS_UCDH_EMPLOYEE, IS_UCD_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_UCDH_EMPLOYEE AS IS_PREVIOUS_HS_EMPLOYEE, IS_PREVIOUS_UCDH_EMPLOYEE, IS_PREVIOUS_UCD_EMPLOYEE, IS_STUDENT, START_DATE, END_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, BANNER_ID, BANNER_START, BANNER_END, CAMPUS_PPS_ID, CAMPUS_PPS_START, CAMPUS_PPS_END, EXTERNAL_ID, EXTERNAL_START, EXTERNAL_END, UCDH_AD_ID, UCDH_AD_START, UCDH_AD_END, KERBEROS_ID, KERBEROS_START, KERBEROS_END, MOTHRA_ID, MOTHRA_START, MOTHRA_END, PPS_ID, PPS_START, PPS_END, STUDENT_ID, STUDENT_START, STUDENT_END, STUDENT_MAJOR, STUDENT_MAJOR_NAME, UC_PATH_ID, UC_PATH_INSTITUTION, UC_PATH_TYPE, UC_PATH_PERCENT, UC_PATH_REPRESENTATION, UC_PATH_START, UC_PATH_END, PROCESSED, CREATED_ON, CREATED_BY, CREATED_FROM, UPDATE_CT, UPDATED_ON, UPDATED_BY, UPDATED_FROM FROM INCOMING WHERE PROCESSED IS NULL ORDER BY TRANSACTION_ID";
	private static final String USER_ID = System.getProperty("user.name");
	private final Log log = LogFactory.getLog(getClass().getName());
	private String ipAddress = "Unkown Host";
	private String jobId = "";
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private HttpClient client = null;
	private Map<String,String> fieldMap = new HashMap<String,String>();
	private String dbDriver = null;
	private String dbURL = null;
	private String dbUser = null;
	private String dbPassword = null;
	private String up2dateService = null;
	private String publisherId = null;
	private String fieldList = null;
	private int recordsRead = 0;
	private int recordsUpdated = 0;
	private int updatesPosted = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		processBegin(batchJobInstanceId);
		while (rs.next()) {
			processTransaction();
		}
		return processEnd();
	}

	private void processBegin(int batchJobInstanceId) throws Exception {
		log.info("NewTransactionPost starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// establish Job ID
		jobId = "" + batchJobInstanceId;
		log.info("jobId = " + jobId);

		// verify dbDriver
		if (StringUtils.isEmpty(dbDriver)) {
			throw new IllegalArgumentException("Required property \"dbDriver\" missing or invalid.");
		} else {
			log.info("dbDriver = " + dbDriver);
		}
		// verify dbUser
		if (StringUtils.isEmpty(dbUser)) {
			throw new IllegalArgumentException("Required property \"dbUser\" missing or invalid.");
		} else {
			log.info("dbUser = " + dbUser);
		}
		// verify dbPassword
		if (StringUtils.isEmpty(dbPassword)) {
			throw new IllegalArgumentException("Required property \"dbPassword\" missing or invalid.");
		} else {
			log.info("dbPassword = ********");
		}
		// verify dbURL
		if (StringUtils.isEmpty(dbURL)) {
			throw new IllegalArgumentException("Required property \"dbURL\" missing or invalid.");
		} else {
			log.info("dbURL = " + dbURL);
		}
		// verify up2dateService
		if (StringUtils.isEmpty(up2dateService)) {
			throw new IllegalArgumentException("Required property \"up2dateService\" missing or invalid.");
		} else {
			log.info("up2dateService = " + up2dateService);
		}
		// verify publisherId
		if (StringUtils.isEmpty(publisherId)) {
			throw new IllegalArgumentException("Required property \"publisherId\" missing or invalid.");
		} else {
			log.info("publisherId = " + publisherId);
		}
		// verify fieldList
		if (StringUtils.isEmpty(fieldList)) {
			throw new IllegalArgumentException("Required property \"fieldList\" missing or invalid.");
		} else {
			log.info("fieldList = " + fieldList);
		}
		// establish IP address
		try {
			ipAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			log.error("Exception encountered attempting to obtain local host address: " + e, e);;
		}
		log.info("IP Address = " + ipAddress);

		// establish field map
		String[] field = fieldList.split(",");
		for (int i=0;i<field.length;i++) {
			String[] parts = field[i].split(":");
			fieldMap.put(parts[0], parts[1]);
		}

		// establish HTTP Client
		client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// connect to database
		Class.forName(dbDriver);
		conn = DriverManager.getConnection(dbURL, dbUser, dbPassword);
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		rs = stmt.executeQuery(SQL);
	}

	private List<BatchJobServiceStatistic> processEnd() throws Exception {
		// close database connection
		rs.close();
		stmt.close();
		conn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (recordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("IAM transaction records read", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsRead + "")));
		}
		if (recordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("IAM transaction records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsUpdated + "")));
		}
		if (updatesPosted > 0) {
			stats.add(new BatchJobServiceStatistic("Up2Date transactions posted", BatchJobService.FORMAT_INTEGER, new BigInteger(updatesPosted + "")));
		}

		// end job
		log.info(" ");
		log.info("NewTransactionPost complete.");

		return stats;
	}

	/**
	 * <p>Processes the IAM transaction.</p>

	 * @throws SQLException 
	 */
	private void processTransaction() throws SQLException {
		recordsRead++;

		// post to Up2Date
		List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
		urlParameters.add(new BasicNameValuePair("_pid", publisherId));
		urlParameters.add(new BasicNameValuePair("_jid", jobId));
		urlParameters.add(new BasicNameValuePair("_tid", rs.getString("TRANSACTION_ID")));
		urlParameters.add(new BasicNameValuePair("_action", rs.getString("ACTION")));
		Iterator<String> i = fieldMap.keySet().iterator();
		while (i.hasNext()) {
			String name = i.next();
			String value = rs.getString(name);
			if (StringUtils.isNotEmpty(value)) {
				urlParameters.add(new BasicNameValuePair(fieldMap.get(name), value.trim()));
			}
		}
		postToService(urlParameters);
		updatesPosted++;

		// update transaction record
		try {
			rs.updateTimestamp("PROCESSED", new Timestamp(new Date().getTime()));
			rs.updateInt("UPDATE_CT", rs.getInt("UPDATE_CT") + 1);
			rs.updateTimestamp("UPDATED_ON", new Timestamp(new Date().getTime()));
			rs.updateString("UPDATED_BY", USER_ID);
			rs.updateString("UPDATED_FROM", ipAddress);
			rs.updateRow();
			recordsUpdated++;
		} catch (Exception e) {
			log.error("Exception encountered attempting to update transaction: " + e, e);
		}
	}

	private void postToService(List<NameValuePair> urlParameters) {
		HttpPost post = new HttpPost(up2dateService + "/publish");
		String resp = "";
		int rc = 0;
		try {
			post.setEntity(new UrlEncodedFormEntity(urlParameters));
			if (log.isDebugEnabled()) {
				log.debug("Posting to the following URL: " + up2dateService + "/publish");
				log.debug("Posting the following parameters: " + urlParameters);
			}
			HttpResponse response = client.execute(post);
			rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP Response Code: " + rc);
			}
			if (rc == 200) {
				HttpEntity entity = response.getEntity();
				if (entity != null) {
					resp = EntityUtils.toString(entity);
					if (log.isDebugEnabled()) {
						log.debug("HTTP Response Length: " + resp.length());
						log.debug("HTTP Response: " + resp);
					}
				}
			} else {
				log.error("Invalid response code (" + rc + ") encountered accessing to URL " + up2dateService + "/publish");
				try {
					resp = EntityUtils.toString(response.getEntity());
				} catch (Exception e) {
					// no one cares
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing URL " + up2dateService + "/publish", e);
		}
	}

	/**
	 * @return the dbDriver
	 */
	public String getDbDriver() {
		return dbDriver;
	}

	/**
	 * @param dbDriver the dbDriver to set
	 */
	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	/**
	 * @return the dbURL
	 */
	public String getDbURL() {
		return dbURL;
	}

	/**
	 * @param dbURL the dbURL to set
	 */
	public void setDbURL(String dbURL) {
		this.dbURL = dbURL;
	}

	/**
	 * @return the dbUser
	 */
	public String getDbUser() {
		return dbUser;
	}

	/**
	 * @param dbUser the dbUser to set
	 */
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	/**
	 * @return the dbPassword
	 */
	public String getDbPassword() {
		return dbPassword;
	}

	/**
	 * @param dbPassword the dbPassword to set
	 */
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	/**
	 * @return the up2dateService
	 */
	public String getUp2dateService() {
		return up2dateService;
	}

	/**
	 * @param up2dateService the up2dateService to set
	 */
	public void setUp2dateService(String up2dateService) {
		this.up2dateService = up2dateService;
	}

	/**
	 * @return the publisherId
	 */
	public String getPublisherId() {
		return publisherId;
	}

	/**
	 * @param publisherId the publisherId to set
	 */
	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}

	/**
	 * @return the fieldList
	 */
	public String getFieldList() {
		return fieldList;
	}

	/**
	 * @param fieldList the fieldList to set
	 */
	public void setFieldList(String fieldList) {
		this.fieldList = fieldList;
	}
}
