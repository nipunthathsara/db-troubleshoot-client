/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.test;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Testor implements Runnable {
    private final static Logger log = Logger.getLogger(Testor.class);

    @Override
    public void run() {
        long clientStopTime = System.currentTimeMillis() + (2 * 60 * 60 * 1000);
        boolean run = true;
        Connection connection = null;
        while (run) {
            if (connection == null) {
                connection = App.getConnection();
            }
            long startTime;
            long endTime;
            try {
                Statement statement = connection.createStatement();
                startTime = System.currentTimeMillis();
                statement.executeQuery(App.properties.getProperty("testQuery"));
                endTime = System.currentTimeMillis();
                log.info("Test query execution time : " + Long.toString(endTime - startTime));

                startTime = System.currentTimeMillis();
                ResultSet resultSet;
                resultSet = statement.executeQuery(App.properties.getProperty("time.measure.query"));
                while (resultSet.next()){
                    // Some JDBC drivers doesn't load all the data unless you iterate through the result set.
                }
                endTime = System.currentTimeMillis();
                log.info("Query execution time : " + Long.toString(endTime - startTime));
            } catch (SQLException e) {
                log.error("Error while executing the time measuring query.", e);
            }
            try {
                Thread.currentThread().sleep(Integer.parseInt(App.properties.getProperty("time.measure.query.interval.milis")));
            } catch (InterruptedException e) {
                log.error(Thread.currentThread().getName() + " Error while sleeping the time interval tester.", e);
            }
            if (clientStopTime <= System.currentTimeMillis()) {
                run = false;
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("Error while closing the connection.", e);
                }
            }
        }
    }
}
