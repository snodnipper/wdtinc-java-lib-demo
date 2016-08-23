package uk.os.wdtinc.demo.impl;

public class Entry {

    private final int mZoomLevel;
    private final int mColumn;
    private final int mRow;
    private final String mId;
    private final String mGrid;
    private final byte[] mVector;

    public Entry(int zoomLevel, int column, int row, String id, String gridId, byte[] vector) {
        mZoomLevel = zoomLevel;
        mColumn = column;
        mRow = row;
        mId = id;
        mGrid = gridId;
        mVector = vector;
    }

    public int getZoomLevel() {
        return mZoomLevel;
    }

    public int getColumn() {
        return mColumn;
    }

    public int getRow() {
        return mRow;
    }

    public String getId() {
        return mId;
    }

    public String getGrid() {
        return mGrid;
    }

    public byte[] getVector() {
        return mVector;
    }
}
