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

        private final String url = "jdbc:postgresql://localhost:5432/timetable_test";
        private final String user = "postgres";
        private final String password = "12345";

        /**
         * Connect to the PostgreSQL database
         *
         * @return a Connection object
         */
        public Connection connect() {
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(url, user, password);
                System.out.println("Connected to the PostgreSQL server successfully.");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

            return conn;
        }

        /**
         * @param args the command line arguments
         */
        public static void main(String[] args) {
            Main app = new Main();
            try (Scanner scanner = new Scanner(new File("D:\\Project\\schedule\\people.csv"));) {
                while (scanner.hasNextLine()) {

                    String sql = "INSERT INTO data1 (id) values (?)";
                    PreparedStatement statement = app.connect().prepareStatement(sql);
                    statement.setInt(1, Integer.parseInt(app.getRecordFromLine(scanner.nextLine())));
                    statement.executeUpdate();
                    statement.close();
                    app.connect().close();
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

    }

