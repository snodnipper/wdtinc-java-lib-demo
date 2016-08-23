package uk.os.wdtinc.demo.impl;

import com.vividsolutions.jts.geom.Point;

/**
 * Google tiling scheme with the origin at the top left, which is also the origin for a vector tile.
 */
public class CoordinateConversion {

    /**
     * The point coordinates are ultimately in respect to the tile coordinates.
     *
     * Note: tile coordinates are assumed to be in the Google tiling scheme
     *
     * @param z zoom tile coordinate
     * @param x row tile coordinate
     * @param y column tile coordinate
     * @param point
     * @param measurementSpace - pure filth - e.g. 256 or 4096
     */
    public static double[] toLatLon(int z, int x, int y, Point point, double measurementSpace) {
        double x_tile_coordinate = (1 / measurementSpace) * point.getX();
        double y_tile_coordinate = 1 - (1 / measurementSpace * point.getY());

        double xf = x + x_tile_coordinate;
        double yf = (y + 1) - y_tile_coordinate;

        double lat = tile2lat(yf, z);
        double lon = tile2lon(xf, z);

        return new double[]{lat, lon};
    }

    public static double[] toLatLon(int z, int x, int y, Point point) {
        return toLatLon(z, x, y, point, 256F);
    }

    // x is normally an int _but_ within the tile is a fraction
    public static double tile2lon(double x, int z) {
        return x / Math.pow(2.0, z) * 360.0 - 180;
    }

    public static double tile2lat(double y, int z) {
        double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

    public static double tile2lon(int x, int z) {
        return x / Math.pow(2.0, z) * 360.0 - 180;
    }

    public static double tile2lat(int y, int z) {
        double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }

    /**
     * @param zoom level for the TMS to Google coordinate conversion
     * @param column the X TMS coordinate
     * @param row the Y TMS coordinate
     * @return zxy array with Google coordinates
     */
    public static int[] fromTms(int zoom, int column, int row) {
        return new int[]{ zoom, column, flipY(row, zoom) };
    }

    public static int[] toTms(int zoom, int column, int row) {
        // same function but named for clarity
        return fromTms(zoom, column, row);
    }

    // TODO private
    public static int flipY(int y, int zoom) {
        return (int) (Math.pow(2, zoom) - y - 1);
    }
}
