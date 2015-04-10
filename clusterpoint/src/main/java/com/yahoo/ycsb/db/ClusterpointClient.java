/**
 * Clusterpoint client binding for YCSB.
 *
 * Submitted by Normunds Pureklis on 3/30/2015.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_clusterpoint.java
 *
 */

//package main.java.com.yahoo.ycsb.db;
package com.yahoo.ycsb.db;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;

import com.clusterpoint.api.CPSConnection;
import com.clusterpoint.api.CPSRequest;
import com.clusterpoint.api.CPSResponse;
import com.clusterpoint.api.request.CPSAlternativesRequest;
import com.clusterpoint.api.request.CPSListFacetsRequest;
import com.clusterpoint.api.request.CPSListLastRequest;
import com.clusterpoint.api.request.CPSListPathsRequest;
import com.clusterpoint.api.request.CPSListWordsRequest;
import com.clusterpoint.api.request.CPSLookupRequest;
import com.clusterpoint.api.request.CPSModifyRequest;
import com.clusterpoint.api.request.CPSSearchDeleteRequest;
import com.clusterpoint.api.request.CPSSearchRequest;
import com.clusterpoint.api.request.CPSStatusRequest;
import com.clusterpoint.api.request.CPSDeleteRequest;
import com.clusterpoint.api.response.CPSAlternativesResponse;
import com.clusterpoint.api.response.CPSListFacetsResponse;
import com.clusterpoint.api.response.CPSListLastRetrieveFirstResponse;
import com.clusterpoint.api.response.CPSListPathsResponse;
import com.clusterpoint.api.response.CPSListWordsResponse;
import com.clusterpoint.api.response.CPSLookupResponse;
import com.clusterpoint.api.response.CPSModifyResponse;
import com.clusterpoint.api.response.CPSSearchDeleteResponse;
import com.clusterpoint.api.response.CPSSearchResponse;
import com.clusterpoint.api.response.CPSStatusResponse;
import com.clusterpoint.api.response.alternatives.CPSAlternativesAlternatives;
import com.clusterpoint.api.response.listfacets.CPSListFacetsContent.Facet;
import com.clusterpoint.api.response.listwords.CPSListWordsContent;
import com.clusterpoint.api.response.search.CPSSearchFacet;
import com.clusterpoint.api.response.status.CPSStatusContent;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * Clusterpoint client for YCSB framework.
 * 
 * Properties to set:
 * 
 * clusterpoint.url=clusterpoint://localhost:27017 clusterpoint.database=ycsb
 * clusterpoint.writeConcern=normal
 * 
 * @author Normunds Pureklis
 */
public class ClusterpointClient extends DB {

	/** Used to include a field in a response. */
	protected static final Integer INCLUDE = Integer.valueOf(1);

	/** The account to access. */
	private String account;

	/** The database to access. */
	private String database;

	/** Clusterpoint connection instance. */
	private CPSConnection clusterpoint;

	/**
	 * Count the number of times initialized to teardown on the last
	 * {@link #cleanup()}.
	 */
	private static final AtomicInteger initCount = new AtomicInteger(0);

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */

	private String threadId;

	private static Map<String, Float> cpsTime = new HashMap();
	private static Map<String, Float> reqTime = new HashMap();
	private static Map<String, Float> netTime = new HashMap();

	@Override
	public void init() throws DBException {
		threadId = "Thread_" + initCount.incrementAndGet();
		setThreadTimer(threadId);
		synchronized (INCLUDE) {

			// initialize Clusterpoint api driver
			Properties props = getProperties();
			String url = props.getProperty("clusterpoint.url",
					"tcp://localhost:15006");
			String user = props.getProperty("clusterpoint.user", "user");
			String password = props.getProperty("clusterpoint.password",
					"password");
			database = props.getProperty("clusterpoint.database", "ycsb");
			account = props.getProperty("clusterpoint.account", "1");

			// System.out.println("Url: " + url);
			// System.out.println("User: " + user);
			// System.out.println("Password: " + password);
			// System.out.println("Database: " + database);
			// System.out.println("Account: " + account);

			try {
				System.out.println("new database url = " + url);
				clusterpoint = new CPSConnection(url, database, user, password,
						account);
				System.out.println("clusterpoint connection created with "
						+ url);
			} catch (Exception e1) {
				System.err
						.println("Could not initialize Clusterpoint connection pool for Loader: "
								+ e1.toString());
				e1.printStackTrace();
				return;
			}
		}
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		System.out.println("Thread " + threadId + " clenup!");
		System.out.println("Thread time: " + cpsTime.get(threadId));
		System.out.println("Thread request time: " + reqTime.get(threadId));
		System.out.println("Thread network time: " + netTime.get(threadId));
		if (initCount.decrementAndGet() <= 0) {
			float totTime = 0;
			float totReqTime = 0;
			float totNetTime = 0;
			for (String key : cpsTime.keySet())
				totTime += cpsTime.get(key);
			System.out.println("Total time: " + totTime);
			for (String key : reqTime.keySet())
				totReqTime += reqTime.get(key);
			System.out.println("Total request time: " + totReqTime);
			for (String key : netTime.keySet())
				totNetTime += netTime.get(key);
			System.out.println("Total network time: " + totNetTime);
		}

		try {
			clusterpoint.close();
		} catch (Exception e1) {
			System.err.println("Could not close Clusterpoint connection pool: "
					+ e1.toString());
			e1.printStackTrace();
			return;
		}
	}

	private void setThreadTimer(String id) {
		cpsTime.put(id, (float) 0);
		reqTime.put(id, (float) 0);
		netTime.put(id, (float) 0);
	}

	@SuppressWarnings("deprecation")
	private static CharSequenceTranslator translator = StringEscapeUtils.ESCAPE_XML;

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int delete(String table, String key) {
		try {
			String ids[] = { key };
			CPSDeleteRequest delete_req = new CPSDeleteRequest(ids);
			CPSModifyResponse delete_resp = (CPSModifyResponse) clusterpoint
					.sendRequest(delete_req);
			return delete_resp.getErrorHandler() == null ? 0 : 1;
		} catch (Exception e) {
			// System.err.println(e.toString());
			return 1;
		} finally {
		}
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		int ret = 1;
		float time = 0;
		float req_time = 0;
		float net_time = 0;
		// cpsTime.put(threadId, 10);
		// cpsTime.put(threadId, (cpsTime.get(threadId) + 1));
		try {
			CPSModifyRequest req = new CPSModifyRequest("insert");
			List<String> docs = new ArrayList<String>();

			StringBuilder document = new StringBuilder("<document><id>"
					+ translator.translate(key) + "</id>");
			for (String k : values.keySet()) {
				// r.put(k, values.get(k).toArray());
				document.append("<" + k + ">"
						+ translator.translate(values.get(k).toString()) + "</"
						+ k + ">");
			}
			document.append("</document>");
			docs.add(document.toString());

			req.setStringDocuments(docs);

			CPSModifyResponse resp = (CPSModifyResponse) clusterpoint
					.sendRequest(req);
			time = resp.getSeconds();
			req_time = clusterpoint.getLastRequestDuration();
			net_time = clusterpoint.getLastNetworkDuration();
			ret = resp.getErrorHandler() == null ? 0 : 1;
		} catch (Exception e) {
			// e.printStackTrace();
			ret = 1;
		} finally {
		}
		cpsTime.put(threadId, (cpsTime.get(threadId) + time));
		reqTime.put(threadId, (reqTime.get(threadId) + time));
		netTime.put(threadId, (netTime.get(threadId) + time));
		return ret;
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	@SuppressWarnings("unchecked")
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		try {
			String ids[] = { key };
			Map<String, String> list = new HashMap<String, String>();
			if (fields != null) {
				Iterator<String> iter = fields.iterator();
				while (iter.hasNext()) {
					list.put("document/" + iter.next().toString(), "yes");
				}
			} else {
				list.put("document", "yes");
			}

			CPSLookupRequest req = new CPSLookupRequest(ids, list);
			CPSLookupResponse resp = (CPSLookupResponse) clusterpoint
					.sendRequest(req);
			List<Element> docs = resp.getDocuments();

			Iterator<Element> it = docs.iterator();
			while (it.hasNext()) {
				Element el = it.next();
				el = (Element) el.getFirstChild();
				NodeList docNodes = el.getElementsByTagName("*");
				Node curChild = null;
				for (int i = 0; i < docNodes.getLength(); i++) {
					curChild = docNodes.item(i);
					ByteIterator content = new com.yahoo.ycsb.StringByteIterator(
							curChild.getTextContent());
					result.put(curChild.getNodeName(), content);
				}
			}
			return resp.getFound() > 0 ? 0 : 1;
		} catch (Exception e) {
			// System.err.println(e.toString());
			return 1;
		} finally {
		}
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		try {
			CPSModifyRequest req = new CPSModifyRequest("partial-replace");
			List<String> docs = new ArrayList<String>();

			StringBuilder document = new StringBuilder("<document><id>" + key
					+ "</id>");
			for (String k : values.keySet()) {
				document.append("<" + k + ">"
						+ translator.translate(values.get(k).toString()) + "</"
						+ k + ">");
			}
			document.append("</document>");
			docs.add(document.toString());
			// System.err.println("Document: " + document.toString());
			req.setStringDocuments(docs);

			// clusterpoint.setDebug(true);
			CPSModifyResponse resp = (CPSModifyResponse) clusterpoint
					.sendRequest(req);
			return resp.getErrorHandler() == null ? 0 : 1;
		} catch (Exception e) {
			// e.printStackTrace();
			return 1;
		} finally {
		}
	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

		CPSSearchRequest req = null;
		try {
			req = new CPSSearchRequest("<id>&gt;=" + startkey + "</id>", 0,
					recordcount);
		} catch (Exception e) {
			e.printStackTrace();
		}
		CPSSearchResponse resp = null;
		try {
			resp = (CPSSearchResponse) clusterpoint.sendRequest(req);
		} catch (TransformerFactoryConfigurationError e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<Element> docs = resp.getDocuments();
		Iterator<Element> it = docs.iterator();

		while (it.hasNext()) {
			Element el = it.next();
			HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();
			NodeList docNodes = el.getElementsByTagName("*");
			Node curChild = null;
			for (int i = 0; i < docNodes.getLength(); i++) {
				curChild = docNodes.item(i);
				resultMap.put(
						curChild.getNodeName(),
						new com.yahoo.ycsb.StringByteIterator(curChild
								.getTextContent()));
			}
			result.add(resultMap);
		}
		return resp.getFound() > 0 ? 0 : 1;

	}
}
