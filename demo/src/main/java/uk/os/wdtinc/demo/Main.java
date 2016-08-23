package uk.os.wdtinc.demo;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.*;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerBuild;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerParams;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import uk.os.wdtinc.demo.impl.Entry;
import uk.os.wdtinc.demo.impl.MbTiles;
import uk.os.wdtinc.demo.impl.StorageImpl;
import uk.os.wdtinc.demo.impl.VectorTileUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getSimpleName());

    /** Default MVT parameters */
    private static final MvtLayerParams DEFAULT_MVT_PARAMS = new MvtLayerParams();

    /** Do not filter tile geometry */
    private static final IGeometryFilter ACCEPT_ALL_FILTER = geometry -> true;

    private static GeometryFactory geomFactory = new GeometryFactory();

    public static void main(String[] args) {
        final int minZoom = 5;
        final int maxZoom = 5;
        final String layerName = "Boundarylinehistoriccounties_regiongeojson";
        final File outputFile = new File("demo_output.mbtiles");
        final String json = String.format("{ \"vector_layers\": [ { \"id\": \"%s\", \"description\": \"\", \"minzoom\": %d, \"maxzoom\": %d, \"fields\": { \"Name\": \"String\", \"Area_Descr\": \"String\" } } ]}", layerName, minZoom, maxZoom);

        MbTiles.initialise(outputFile);
        MbTiles.writeMetadata(outputFile, "OS", "0,51,5", "", "pbf", maxZoom, minZoom, "name", json);

        StorageImpl borderData = new StorageImpl("data/historical.mbtiles");

        borderData.queryMbtilesEntries(5).subscribe(entry -> {
            LOGGER.log(Level.INFO, "onNext");
            try {
                boolean passThrough = true; // TODO CHANGE ME - passthrough just emits input
                if (passThrough) {
                    MbTiles.writeTile(outputFile, entry);
                } else {
                    List<Geometry> geoms = getGeometries(geomFactory, entry);

                    // Create input geometry
                    final GeometryFactory geomFactory1 = new GeometryFactory();

                    // Build tile envelope
                    final Envelope tileEnvelope = new Envelope(0d, 4096d, 0d, 4096d);

                    // Build MVT tile geometry
                    final TileGeomResult tileGeom = JtsAdapter.createTileGeom(geoms, tileEnvelope, geomFactory1,
                            DEFAULT_MVT_PARAMS, ACCEPT_ALL_FILTER);

                    // Create MVT layer
                    byte[] result = encodeMvt(DEFAULT_MVT_PARAMS, tileGeom, layerName);

                    Entry newEntry = new Entry(entry.getZoomLevel(), entry.getColumn(), entry.getRow(),
                            entry.getId(), entry.getGrid(), result);
                    MbTiles.writeTile(outputFile, newEntry);
                    print(geomFactory, newEntry, true);
                    LOGGER.log(Level.INFO, "Geom size: " + geoms.size());
                }

            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "problem fetching geometry", e);
            }
        });
    }

    private static byte[] encodeMvt(MvtLayerParams mvtParams, TileGeomResult tileGeom, String layerName) {
        return encodeMvt2(mvtParams, tileGeom, layerName).toByteArray();
    }

    private static VectorTile.Tile encodeMvt2(MvtLayerParams mvtParams, TileGeomResult tileGeom, String layerName) {

        // Build MVT
        final VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();

        // Create MVT layer
        final VectorTile.Tile.Layer.Builder layerBuilder = MvtLayerBuild.newLayerBuilder(layerName, mvtParams);
        final MvtLayerProps layerProps = new MvtLayerProps();
        final UserDataIgnoreConverter ignoreUserData = new UserDataIgnoreConverter();

        // MVT tile geometry to MVT features
        final List<VectorTile.Tile.Feature> features = JtsAdapter.toFeatures(tileGeom.mvtGeoms, layerProps, ignoreUserData);
        layerBuilder.addAllFeatures(features);
        MvtLayerBuild.writeProps(layerBuilder, layerProps);

        // Build MVT layer
        final VectorTile.Tile.Layer layer = layerBuilder.build();

        // Add built layer to MVT
        tileBuilder.addLayers(layer);

        /// Build MVT
        return tileBuilder.build();
    }


    private static List<Geometry> getGeometries(GeometryFactory geomFactory, Entry entry) throws IOException {
        byte[] bytes = entry.getVector();
        InputStream is = new ByteArrayInputStream(VectorTileUtil.getUncompressedVectorTileFrom(bytes));
        return MvtReader.loadMvt(
                is,
                geomFactory,
                new TagKeyValueMapConverter());
    }

    private static void print(GeometryFactory geomFactory, Entry entry) throws IOException {
        print(geomFactory, entry, false);
    }
    private static void print(GeometryFactory geomFactory, Entry entry, boolean includeCoordinates) throws IOException {
        byte[] bytes = entry.getVector();
        InputStream is = new ByteArrayInputStream(VectorTileUtil.getUncompressedVectorTileFrom(bytes));
        List<Geometry> geoms = MvtReader.loadMvt(
                is,
                geomFactory,
                new TagKeyValueMapConverter());

        if (includeCoordinates) {
            for (Geometry geometry : geoms) {
                for (Coordinate coordinate : geometry.getCoordinates()) {
                    LOGGER.log(Level.INFO, coordinate.toString());
                }
            }
        }
        LOGGER.log(Level.INFO, "Total geometries: " + geoms.size());
    }
}
