package com.test.Database_Testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class Missing_BFI {

    private Connection conn;
    private static final String URL = "jdbc:mysql://apollo2.humanbrain.in:3306/HBA_V2";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "Health#123";

    @BeforeClass
    public void setup() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            System.out.println("Database connection established.");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Database connection failed.");
        }
    }

    @Test
    public void displayMissingPositionIndexes() {
        String biosampleid = System.getProperty("biosampleid");

        if (biosampleid == null || biosampleid.trim().isEmpty()) {
            throw new RuntimeException("biosampleid system property is missing or empty.");
        }

        System.out.println("Running for biosampleid: " + biosampleid);

        Set<Integer> retrievedIndexes = new HashSet<>();
        int actualEndIndex = 0;
        int missingCount = 0;

        try {
            // Get all position indexes sorted
            String query = "SELECT positionindex FROM section WHERE jp2Path LIKE ? ORDER BY positionindex ASC";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setString(1, "%/" + biosampleid + "/BFIW/%");
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        int current = rs.getInt("positionindex");

                        if (current < 10000) {  // Ignore dummy values 10000 and above
                            retrievedIndexes.add(current);
                            if (current > actualEndIndex) {
                                actualEndIndex = current; // Update max only if less than 10000
                            }
                        }
                    }
                }
            }

            // Print missing indexes
            System.out.println("Missing position indexes (up to " + actualEndIndex + "):");
            for (int i = 1; i <= actualEndIndex; i++) {
                if (!retrievedIndexes.contains(i)) {
                    System.out.println(i);
                    missingCount++;
                }
            }

            System.out.println("\nTotal missing position indexes: " + missingCount);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error executing query.");
        }
    }

    @AfterClass
    public void tearDown() {
        try {
            if (conn != null) conn.close();
            System.out.println("Database connection closed.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
