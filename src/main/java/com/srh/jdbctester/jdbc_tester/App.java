package com.srh.jdbctester.jdbc_tester;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;



/**
 * Hello world!
 *
 */
public class App 
{
	private static Logger LOGGER = Logger.getLogger(App.class.getName());
	private static String KEY_JDBC_URL = "jdbc_url";
	private static String KEY_JDBC_USER = "jdbc_user";
	private static String KEY_JDBC_PASSWORD = "jdbc_password";
	private static String KEY_JDBC_DRIVER_CLASS_NAME= "jdbc_driver_classname";
	private static String KEY_CALL_COUNTER = "call_counter";
	private static String KEY_SCHEMA = "call_schema";
	private static String KEY_CATALOG = "call_catalog";
	

    public static void main( String[] args ) throws Exception
    {
        if (args.length == 0){
        	usage();
        	return;
        }
//        Class.forName("");
        String fileLocation = args[0];
        App pqt = new App();
        Properties props = pqt.loadPropertiesFile(fileLocation);
        
        pqt.performAction(props);
        
        

    }
    
    public void performAction(Properties props) throws Exception{
        LOGGER.info("going to create ds");
        DataSource ds = this.createDataSource(props);
        
        int counter = (Integer)props.getOrDefault(KEY_CALL_COUNTER, 10);
        
        String schema = props.getProperty(KEY_SCHEMA);
        String catalog = props.getProperty(KEY_CATALOG);

        Connection connection = null;
        ResultSet resultset = null;

        try {

        for (int i = 1; i <= counter; i++) {
        	LOGGER.info("Counter ="+i);
            connection = ds.getConnection();
            DatabaseMetaData dbMetadata = connection.getMetaData();
            long startTime = System.currentTimeMillis();
            resultset = dbMetadata.getTables(catalog, schema, null, null);
            long endTime = System.currentTimeMillis();
            LOGGER.info("Time Taken to call getTables" + (endTime - startTime));
		}
        }catch(Exception ex ) {
        	LOGGER.info("ERROR executing connection call");
        	throw ex;
        }finally {
        	close(connection, null, resultset);
        }
    	
    }
	public static void usage(){
    	LOGGER.severe("java -cp <classpath> com.srh.jdbctester.jdbc_tester.App <propertiesLocation>");
    }
	
	private DataSource createDataSource(Properties props) {
		if(props == null)
			throw new IllegalArgumentException("properties file is required to run this program");
		
		String jdbc_url = props.getProperty(KEY_JDBC_URL);
		String user_name = props.getProperty(KEY_JDBC_USER);
		String password = props.getProperty(KEY_JDBC_PASSWORD);
		String driverClassName = props.getProperty(KEY_JDBC_DRIVER_CLASS_NAME);
		
		LOGGER.info("jdbc_url="+jdbc_url);
		LOGGER.info("user_name="+user_name);
		LOGGER.info("driverClassName="+driverClassName);
		
		if(jdbc_url == null || jdbc_url.trim().isEmpty())
			throw new IllegalArgumentException("jdbc_url property is missing");
		
		if(user_name == null || user_name.trim().isEmpty())
			throw new IllegalArgumentException("user name property is missing");
		
		if(password == null || password.trim().isEmpty())
			throw new IllegalArgumentException("password property is missing");
		
		
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(jdbc_url);
        dataSource.setUsername(user_name);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverClassName);
        return dataSource;		
		
	}
	
    
    private Properties loadPropertiesFile(String fileLocation) {
    	Properties pr = new Properties();
    	FileInputStream is = null;
    	try{
    	is = new FileInputStream(fileLocation);
    	pr.load(is);
    	}catch(Exception ex ){
    		ex.printStackTrace();
    	}finally{
    		if(is != null)
    			try{is.close();
    			}catch(Exception ex){
    				ex.printStackTrace();
    			}
    	}
    	return pr;
    }

    private static void close(Connection connection, Statement statement, ResultSet resultSet) {
    	    if (resultSet != null) {
    	      try {
    	        resultSet.close();
    	      } catch (SQLException e) {
    	        // ignore
    	      }
    	    }
    	    if (statement != null) {
    	      try {
    	        statement.close();
    	      } catch (SQLException e) {
    	        // ignore
    	      }
    	    }
    	    if (connection != null) {
    	      try {
    	        connection.close();
    	      } catch (SQLException e) {
    	        // ignore
    	      }
    	    }
    	  }
}
