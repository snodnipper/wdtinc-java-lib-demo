package uk.os.wdtinc.demo.impl;

import com.github.davidmoten.rx.jdbc.Database;
import com.github.davidmoten.rx.jdbc.ResultSetMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import rx.Observable;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StorageImpl {

    private static final Logger LOGGER = Logger.getLogger(StorageImpl.class.getSimpleName());

    private final Database mDataSource;

    public StorageImpl(File file) {
        Connection connection = null;
        try {
            connection = getConnection(file);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "problem establishing a connection", e);
        }

        boolean isConnection = connection != null;
        if (isConnection) {
            mDataSource = Database.from(connection);
        } else {
            mDataSource = null;
        }
    }

    /**
     * @param fileLocation e.g. /Users/snodnipper/Desktop/tilelive/os_borders.mbtiles
     */
    public StorageImpl(String fileLocation) {
        this(new File(fileLocation));
        LOGGER.log(Level.INFO, "StorageImpl: " + new File(fileLocation).getAbsolutePath());
    }

    public void disconnect() {
        if (mDataSource != null) {
            mDataSource.close();
        }
    }

    public int getCount() {
        return mDataSource.select("SELECT COUNT(*) FROM map").getAs(Integer.class).toBlocking().single();
    }

    public Observable<VectorTileConfig> queryConfig() {
        return mDataSource.select("SELECT MIN(zoom_level) as min_zoom_level, MAX(zoom_level) as max_zoom_level FROM tiles;").get(new ResultSetMapper<VectorTileConfig>() {
            @Override
            public VectorTileConfig call(ResultSet rs) throws SQLException {
                return new VectorTileConfig(rs.getInt("min_zoom_level"), rs.getInt("max_zoom_level"));
            }
        });
    }

    public Observable<Entry> queryMbtilesEntries() {
        return mDataSource.select(
                "SELECT zoom_level, tile_column, tile_row, rowid AS tile_id, tile_data " +
                        "FROM tiles")
                .get(rs -> new Entry(
                        rs.getInt("zoom_level"), rs.getInt("tile_column"),
                        CoordinateConversion.flipY(rs.getInt("tile_row"), rs.getInt("zoom_level")),
                        rs.getString("tile_id"),
                        "deleteMe", rs.getBytes("tile_data")));
    }

    public Observable<Entry> queryMbtilesEntries(int z) {
        return mDataSource.select(
                "SELECT zoom_level, tile_column, tile_row, rowid AS tile_id, tile_data " +
                        "FROM tiles " +
                        "WHERE zoom_level = ?")
                .parameter(z)
                .get(rs -> new Entry(
                        rs.getInt("zoom_level"), rs.getInt("tile_column"),
                        CoordinateConversion.flipY(rs.getInt("tile_row"), rs.getInt("zoom_level")),
                        rs.getString("tile_id"),
                        "deleteMe", rs.getBytes("tile_data")));
    }


    public Observable<Entry> queryMbtilesEntry(int z, int x, int y) {
        String key = String.format("%d/%d/%d", z, x, y);
        List<Entry> result = mCache.getIfPresent(key);

        boolean isCacheHit = result != null;
        if (isCacheHit) {
            return Observable.from(result);
        }

        return mDataSource.select(
                "SELECT zoom_level, tile_column, tile_row, rowid AS tile_id, tile_data " +
                        "FROM tiles " +
                        "WHERE zoom_level = ?" +
                        "AND tile_column = ?" +
                        "AND tile_row = ?")
                .parameter(z)
                .parameter(x)
                .parameter(CoordinateConversion.flipY(y, z))
                .get(rs -> new Entry(
                        rs.getInt("zoom_level"), rs.getInt("tile_column"),
                        CoordinateConversion.flipY(rs.getInt("tile_row"), rs.getInt("zoom_level")),
                        rs.getString("tile_id"),
                        "deleteMe", rs.getBytes("tile_data")));
    }

    Cache<String, List<Entry>> mCache = CacheBuilder.newBuilder()
            .concurrencyLevel(4)
            .expireAfterAccess(1, TimeUnit.MINUTES)
            .maximumSize(100)
            .build();

    public Observable<Entry> queryMbtilesEntryWithCache(int z, int x, int y) {
        Observable<Entry> source = queryMbtilesEntry(z, x, y);

        return source;
    }

    public Observable<Integer> queryMaxZoomLevel() {
        return mDataSource.select("SELECT MAX(zoom_level) as max_zoom_level FROM tiles;").get(new ResultSetMapper<Integer>() {
            @Override
            public Integer call(ResultSet rs) throws SQLException {
                return rs.getInt("max_zoom_level");
            }
        });
    }

    private static Connection getConnection(File file) throws SQLException {
        return getConnection(file.getAbsolutePath());
    }

    private static Connection getConnection(String fileLocation) throws SQLException {
        LOGGER.log(Level.INFO, "DB file location " + fileLocation);
        return DriverManager.getConnection("jdbc:sqlite:" + fileLocation);
    }
}