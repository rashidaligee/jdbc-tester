package com.srh.jdbctester.jdbc_tester;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
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
	private static String KEY_DS_TEST = "ds_test";
	private static String KEY_DB_TYPE = "db_type";
	private static String KEY_PRINT_TABLE = "print_table";
	private static String KEY_THREAD_COUNT = "thread_count";
	

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
        
        int counter = Integer.parseInt((String)props.getOrDefault(KEY_CALL_COUNTER, 10));

        LOGGER.info("going to create ds");
        DataSource ds = this.createDataSource(props);

        String schema = props.getProperty(KEY_SCHEMA);
        String catalog = props.getProperty(KEY_CATALOG);

        
        String ds_test = props.getProperty(KEY_DS_TEST);
        
        Connection connection = null;
        ResultSet resultset = null;

        for (int i = 1; i <= counter; i++) {
        try {
        	if(ds_test != null && ds_test.equalsIgnoreCase("true")) {
	        	LOGGER.info("Counter = "+i);
	            connection = ds.getConnection();
	            DatabaseMetaData dbMetadata = connection.getMetaData();
	            long startTime = System.currentTimeMillis();
	            resultset = dbMetadata.getTables(catalog, schema, null, null);
	            long endTime = System.currentTimeMillis();
	            LOGGER.info("Time Taken to call getTables " + (endTime - startTime));
            }else {
            	
            	
            	emulateHiveCalcite(props);
            }
        }catch(Exception ex ) {
        	LOGGER.info("ERROR executing connection call");
        	throw ex;
        }finally {
        	close(connection, null, resultset);
        }
       }
    	
    }

	private void emulateHiveCalcite(Properties props) throws Exception {
        int counter = Integer.parseInt((String)props.getOrDefault(KEY_THREAD_COUNT, 10));

        ExecutorService executors = Executors.newFixedThreadPool(counter);
        
        for (int i = 1; i <= counter; i++) {
			executors.execute(new CalciteRunnable(props));
		}
		
        executors.shutdown();
        
        executors.awaitTermination(30, TimeUnit.SECONDS);
		
	}
	
	public static class CalciteRunnable implements Runnable{

		final String url;
		final String user;
		final String pswd;
		final String driver;
		final String dataBaseType;
        final String schemaName;
        final String catalogName;
        final String printTables;

		public CalciteRunnable(Properties props) {
			this.url = props.getProperty(KEY_JDBC_URL);
			this.user = props.getProperty(KEY_JDBC_USER);
			this.pswd = props.getProperty(KEY_JDBC_PASSWORD);
			this.driver = props.getProperty(KEY_JDBC_DRIVER_CLASS_NAME);
			this.dataBaseType = props.getProperty(KEY_DB_TYPE, "POSTGRES");
	        this.schemaName = props.getProperty(KEY_SCHEMA);
	        this.catalogName = props.getProperty(KEY_CATALOG);
	        this.printTables = props.getProperty(KEY_PRINT_TABLE);
			
		}

		public void run() {
					String threadname = Thread.currentThread().getName();
					
					LOGGER.info(System.currentTimeMillis()+"-"+threadname + " calling calcite");
					DataSource ds = JdbcSchema.dataSource(url, driver, user, pswd);
					SqlDialect jdbcDialect = JdbcSchema.createDialect(SqlDialectFactoryImpl.INSTANCE, ds);
					LOGGER.info(System.currentTimeMillis()+"-"+threadname + " jdbcDialect is created");
					JdbcConvention jc = JdbcConvention.of(jdbcDialect, null, dataBaseType);
					JdbcSchema schema = new JdbcSchema(ds, jc.dialect, jc, catalogName, schemaName);
					Set<String> tableNames = schema.getTableNames();
					LOGGER.info(System.currentTimeMillis()+"-"+threadname + " getTagbles is called");
					if(printTables != null && printTables.trim().equalsIgnoreCase("true")) {
						for (String tableName : tableNames) {
							System.out.print(tableName+",");
						}
						System.out.println();
					}
					LOGGER.info(threadname + " finished");
					
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
