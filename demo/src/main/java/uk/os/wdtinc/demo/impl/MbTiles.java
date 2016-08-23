package uk.os.wdtinc.demo.impl;


import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.Map;
import java.util.TreeMap;

public class MbTiles {

    /**
     * @param file e.g. your_output.mbtiles
     */
    public static void initialise(File file) {
        boolean isClean;
        if (file.exists()) {
            isClean = file.delete();
        } else {
            isClean = true;
        }

        if (!isClean) {
            System.err.println("not clean...");
            return;
        }
        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            URL url = Resources.getResource("mbtiles_schema.sql");
            String text = Resources.toString(url, Charsets.UTF_8);

            statement.executeUpdate(text);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } catch (IOException e) {
            // probably an issue with resources
            System.err.println(e.getMessage());
        }
        finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                System.err.println(e);
            }
        }
    }


    public static void writeMetadata(File file, String attribution, String center, String description, String format,
                                     int maxZoom, int minZoom, String name, String json) {
        Map<String, String> attributes = new TreeMap<>();
        attributes.put("attribution", attribution);
        attributes.put("center", center);
        attributes.put("description", description);
        attributes.put("format", format);
        attributes.put("maxzoom", Integer.toString(maxZoom));
        attributes.put("minzoom", Integer.toString(minZoom));
        attributes.put("name", name);
        attributes.put("json", json);
        writeMetadata(file, attributes);
    }

    public static void writeTile(File file, Entry entry) {
        updateDatabase(file, new Task() {
            @Override
            public void onExecute(Connection connection) throws SQLException {
                try {
                    byte[] entryVector = entry.getVector();
                    byte[] compressed;

                    if (VectorTileUtil.isGZIPStream(entryVector)) {
                        compressed = entryVector;
                    } else {
                        compressed = VectorTileUtil.getCompressedAsGzip(entryVector);
                    }

                    int[] coord = CoordinateConversion.toTms(entry.getZoomLevel(), entry.getColumn(),
                            entry.getRow());

                    // TODO transaction!
                    String INSERT_STATEMENT = "INSERT OR IGNORE INTO images VALUES(?, ?);";
                    PreparedStatement p = connection.prepareStatement(INSERT_STATEMENT);
                    p.setBytes(1, compressed);
                    p.setString(2, entry.getId());
                    p.executeUpdate();

                    String INSERT_STATEMENT2 = "INSERT OR IGNORE INTO map VALUES(?, ?, ?, ?, ?)";
                    PreparedStatement p2 = connection.prepareStatement(INSERT_STATEMENT2);
                    p2.setInt(1, coord[0]);
                    p2.setInt(2, coord[1]); // TODO TODO TODO!!!!
                    p2.setInt(3, coord[2]);
                    p2.setString(4, entry.getId());
                    p2.setString(5, entry.getGrid());
                    p2.executeUpdate();
                } catch (Exception e) {
                    System.err.print("oh dear cannot write");
                    e.printStackTrace();
                }
            }
        });
    }

    private static void initialise(String location) {
        File file = new File(location);
        initialise(file);
    }

    private static void writeMetadata(File file, Map<String, String> attributes) {
        updateDatabase(file, new Task() {
            @Override
            public void onExecute(Connection connection) throws SQLException {
                String INSERT_STATEMENT = "INSERT INTO metadata VALUES(?, ?);";

                for (String key : attributes.keySet()) {
                    PreparedStatement p = connection.prepareStatement(INSERT_STATEMENT);
                    p.setString(1, key);
                    p.setString(2, attributes.get(key));
                    p.executeUpdate();
                }

            }
        });
    }

    interface Task {
        void onExecute(Connection connection) throws SQLException;
    }

    private static boolean updateDatabase(File file, Task task) {
        boolean success = false;

        Connection connection = null;
        try {
            // create a database connection
            connection = DriverManager.getConnection("jdbc:sqlite:" + file.getAbsolutePath());

            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            task.onExecute(connection);
            success = true;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        finally {
            try {
                if (connection != null)
                    connection.close();
            } catch (SQLException e) {
                // connection close failed.
                System.err.println(e);
            }
        }
        return success;
    }
}
