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
                "                    from community_service.channel_member cm\n" +
                "                    join community_service.channel c ON cm.channel_id = c.id \n" +
                "                    INNER JOIN community_service.`member` m ON m.id = cm.member_id \n" +
                "                    INNER JOIN community_service.community c2 ON c.community_id = c2.id \n" +
                "              \t\t\n" +
                "                    where (c2.user_service_tenant_id = ? and cm.channel_id = ? and m.external_reference_id = ? and cm.joined =?) ";

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
        private void generateReport() throws SQLException {
            PreparedStatement statement = connect().prepareStatement(query);
            try (Scanner scanner = new Scanner(new File(filePath))) {
                while (scanner.hasNextLine()) {
                    String [] values = getRecordFromLine(scanner.nextLine()).split(",");
                    statement.setString(1,values[0].trim());
                    statement.setString(2,values[1].trim());
                    statement.setString(3,values[2].trim());
                    statement.setString(4,values[3].trim());
                    System.out.println(statement.toString());
                }

            } catch (FileNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
        public static void main(String[] args) throws SQLException {
            Main app = new Main();
            app.generateReport();
        }

}

