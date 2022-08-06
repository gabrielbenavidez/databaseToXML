import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatDtdWriter;
import org.dbunit.dataset.xml.FlatXmlDataSet;

import com.infotechsoft.rdb.model.DatabaseModelException;
import com.infotechsoft.rdb.model.metadatasvc.MetadataSvcBase;
import com.infotechsoft.rdb.model.metadatasvc.MySqlMetadataService;

public class DBToXML {

	// dbspecs
	static String dbName = "testdb";
	static String tableName = "person";
	static final String DRIVERURL = "com.mysql.jdbc.Driver";
	static String username = "root";
	static String password = "";
	private static final String SSLSUPPRESSION = "?autoRecconect=true&useSSL=false";// necessary to not get warnings in
																					// mysql
	static final String JDBCURL = "jdbc:mysql://127.0.0.1:32775" + SSLSUPPRESSION;

	// xml info
	static String xmlTag = "person"; // table name
	static String query;
	static String xmlOutputName = "testDataSet.xml";
	static boolean connectedToDb = false;

	// connection info
	static MetadataSvcBase mdSvc;
	static Connection jdbcConnection;
	static IDatabaseConnection IDbConnection;
	static StringBuilder strBuilder = new StringBuilder();
	static Statement stmnt;
	static QueryDataSet partialDataSet;
	static IDataSet iDataSet;

	public static void main(String[] args) throws Exception {
		getDriverClass(DRIVERURL);
		jdbcConnection = DriverManager.getConnection(JDBCURL, username, password);
		IDbConnection = new DatabaseConnection(jdbcConnection);
		mdSvc = new MySqlMetadataService(jdbcConnection);
		partialDataSet = createPartialDataSet(IDbConnection);
		// iDataSet = createIDataSet(IDbConnection);
		query = createQuery(dbName, tableName);
		if (!checkQuery(query)) {
			System.out.println("Error executing query");
		} else {
			// createFlatDtd(xmlOutputName, iDataSet);
			createXmlTable(partialDataSet, xmlTag, query, xmlOutputName);
		}
	}

	public static Class<?> getDriverClass(String driverUrl) throws ClassNotFoundException {
		return Class.forName(driverUrl);
	}

	public static QueryDataSet createPartialDataSet(IDatabaseConnection IDbConnection) {
		QueryDataSet pds = new QueryDataSet(IDbConnection);
		return pds;
	}

	public static IDataSet createIDataSet(IDatabaseConnection conn) throws SQLException {
		return conn.createDataSet();
	}

	public static String createQuery(String dbName, String tableName) {
		String queryStatement = dbName + "." + tableName;
		return getSelectAllStatement(queryStatement);
	}

	public static String getSelectAllStatement(String query) {
		return "SELECT * FROM " + query;
	}

	public static boolean checkQuery(String query) {
		boolean queryStatus = false;
		try {
			stmnt = jdbcConnection.createStatement();
			ResultSet rs = stmnt.executeQuery(query);
			if (!rs.next()) {
				queryStatus = false;
			} else {
				queryStatus = true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return queryStatus;
	}

	public static ResultSet executeQuery(String query) {
		ResultSet rs = null;
		try {
			rs = stmnt.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;
	}

	// partial Database export
	public static void createXmlTable(QueryDataSet partialDataSet, String xmlTag, String query, String xmlOutputName)
			throws DataSetException, FileNotFoundException, IOException {
		try {
			partialDataSet.addTable(xmlTag, query);
			flatXmlWriter(partialDataSet);
		} catch (DataSetException e) {
			throw new DatabaseModelException(String.format("DataSetException", e.getMessage()), e);
		} catch (FileNotFoundException e) {
			throw new DatabaseModelException(String.format("FileNotFoundException", e.getMessage()), e);
		} catch (IOException e) {
			throw new DatabaseModelException(String.format("IOException", e.getMessage()), e);
		}
	}

	public static void flatXmlWriter(QueryDataSet partialDataSet)
			throws DataSetException, FileNotFoundException, IOException {
		try {
			FlatXmlDataSet.write(partialDataSet, new FileOutputStream(xmlOutputName));
			System.out.println("xml conversion successful file " + xmlOutputName + " has been created");
		} catch (DataSetException e) {
			throw new DatabaseModelException(String.format("DataSetException", e.getMessage()), e);
		} catch (FileNotFoundException e) {
			throw new DatabaseModelException(String.format("FileNotFoundException", e.getMessage()), e);
		} catch (IOException e) {
			throw new DatabaseModelException(String.format("IOException %s", e.getMessage()), e);
		}
	}

	public static void createFlatDtd(String xmlOutputName, IDataSet iDataSet)
			throws FileNotFoundException, DataSetException {
		Writer out = new OutputStreamWriter(new FileOutputStream(xmlOutputName));
		FlatDtdWriter dataSetWriter = new FlatDtdWriter(out);
		dataSetWriter.write(iDataSet);
		System.out.println("XS conversion successful file " + xmlOutputName + " has been created");
	}

}
