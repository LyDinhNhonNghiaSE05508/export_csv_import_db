package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

        private final String url = "jdbc:sqlserver://localhost:1433;databaseName=DBTestLocal";
        private final String user = "sa";
        private final String password = "123456";
        private final static String filePath ="C:\\Users\\NTQ\\Downloads\\channel_member_only_mysql.csv";
        private final static String query = "select *\n" +
                "                    from community_service.channel_member cm" +
                "                    join community_service.channel c ON cm.channel_id = c.id " +
                "                    INNER JOIN community_service.`member` m ON m.id = cm.member_id " +
                "                    INNER JOIN community_service.community c2 ON c.community_id = c2.id where (c2.user_service_tenant_id = '3d3bfba37a' and cm.channel_id = 4408 and m.external_reference_id = '264431' and cm.joined ='1') ";

        /**
         * Connect to the PostgreSQL database
         *
         * @return a Connection object
         */
        public Connection connect() {
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

            return conn;
        }
        public void saveDataFromCSVToDB() throws SQLException {
            String sql = "INSERT INTO dataRedshift (id) values (?)";
            PreparedStatement statement = connect().prepareStatement(sql);
            try (Scanner scanner = new Scanner(new File(filePath));) {
                while (scanner.hasNextLine()) {
                    statement.setString(1,getRecordFromLine(scanner.nextLine()));
                    System.out.println("Added data!");
                    statement.executeUpdate();

                }
            } catch (FileNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        private String getRecordFromLine(String line) {

            try (Scanner rowScanner = new Scanner(line)) {
                while (rowScanner.hasNext()) {
                    return rowScanner.next();
                }
            }
           return "";
        }
        private void generateQuery() throws SQLException {
            try (Scanner scanner = new Scanner(new File(filePath))) {
                List<String> newQuery = new ArrayList<>();
                while (scanner.hasNextLine()) {
                    String [] values = getRecordFromLine(scanner.nextLine()).split(",");
                    newQuery.add(String.format("or (c2.user_service_tenant_id = '%s' and cm.channel_id = %s and m.external_reference_id = '%s' and cm.joined ='%s')",values[0],values[1],values[2],values[3]));
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        public static void main(String[] args) throws SQLException {
            Main app = new Main();
            app.generateQuery();
        }

}

