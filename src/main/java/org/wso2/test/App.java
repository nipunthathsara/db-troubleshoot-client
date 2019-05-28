package org.wso2.test;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Properties;


/**
 * Hello world!
 */
public class App implements Runnable{
    private static final String CONFIGS_PROPERTIES = "configs.properties";
    private static final String LOG4J_PROPERTIES = "log4j.properties";
    static Properties properties = new Properties();
    private final static Logger log = Logger.getLogger(App.class);

    public static void main(String[] args) {
        log.info("Hello world.");
        initialize();

        // Iterating
        int limit = Integer.parseInt(properties.getProperty("iterations"));
        log.info("Number of iterations : " + limit);
        for (int i = 1; i <= limit; i++) {
            App app = new App ();
            Thread thread = new Thread(app);
            thread.setName("thread" + Integer.toString(i));
            thread.start();
            log.info(thread.getName() + " started.");
        }
    }

    public static void initialize() {
        log.info("Initializing the client...");
        String filePath = Paths.get(".", LOG4J_PROPERTIES).toString();
        log.info("Reading log4j.properties file : " + filePath);
        PropertyConfigurator.configure(filePath);

        filePath = Paths.get(".", CONFIGS_PROPERTIES).toString();
        log.info("Reading properties file : " + filePath);
        try {
            InputStream inputStream = new FileInputStream(filePath);
            if (inputStream != null) {
                properties.load(inputStream);
            } else {
                log.error("Input stream null.");
            }
        } catch (FileNotFoundException e) {
            log.error("Cannot find the configs.properties file.", e);
        } catch (IOException e) {
            log.error("Error while loading the properties from file.", e);
        }
        log.info("Finished initialization.");
    }

    public static Connection getConnection() {
        log.info(Thread.currentThread().getName() + " getConnection");
        Connection connection = null;
        try {
            Class.forName(properties.getProperty("driver.class.name"));
            connection = DriverManager.getConnection(properties.getProperty("jdbc.url"),
                    properties.getProperty("username"), properties.getProperty("password"));
        } catch (ClassNotFoundException e) {
            log.error("Driver not found.", e);
        } catch (SQLException e) {
            log.error("Error while creating the connection", e);
        }

        if (connection != null) {
            log.info("Returning connection.");
            return connection;
        } else {
            log.error("Failed to create a connection.");
            System.exit(0);
            return connection;
        }
    }

    @Override
    public void run() {
        log.info(Thread.currentThread().getName());
        Connection connection = getConnection();
        Statement statement;
        ResultSet resultSet;
        try {
            log.info(Thread.currentThread().getName() + " Executing test query : " + properties.getProperty("testQuery"));
            statement = connection.createStatement();
            resultSet = statement.executeQuery(properties.getProperty("testQuery"));
            // TODO should i iterate through result set?
            log.info(Thread.currentThread().getName() + " Test query success.");
        } catch (SQLException e) {
            log.error(Thread.currentThread().getName() + " Error while test query.", e);
        }

        int sleepTimeMinutes = Integer.parseInt(properties.getProperty(Thread.currentThread().getName() + ".sleeptime.minutes"));
        int sleepTimeMilis = sleepTimeMinutes * 60 * 1000;
        log.info(Thread.currentThread().getName() + " sleeping for : " + sleepTimeMinutes + " minutes.");
        try {
            Thread.currentThread().sleep(sleepTimeMilis);
        } catch (InterruptedException e) {
            log.error(Thread.currentThread().getName() + " Error while sleeping.", e);
        }

        log.info(Thread.currentThread().getName() + " Waking up.");
        String query = properties.getProperty(Thread.currentThread().getName() + ".query");
        log.info(Thread.currentThread().getName() + " Executing : " + query + " after " + sleepTimeMinutes);
        try {
            // TODO check if re-initializing statement is okay?
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);
            // TODO should I iterate through result set?
            log.info(Thread.currentThread().getName() + " Query execution success.");
        } catch (SQLException e) {
            log.error(Thread.currentThread().getName() + " Error while executing the query.", e);
        }
    }
}
