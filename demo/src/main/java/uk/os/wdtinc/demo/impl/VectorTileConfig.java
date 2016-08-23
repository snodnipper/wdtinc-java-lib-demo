package uk.os.wdtinc.demo.impl;

public class VectorTileConfig {

    private final int mMaxZoom;
    private final int mMinZoom;

    public VectorTileConfig(int minZoom, int maxZoom) {
        mMinZoom = minZoom;
        mMaxZoom = maxZoom;
    }

    public int getMaxZoom() {
        return mMaxZoom;
    }

    public int getMinZoom() {
        return mMinZoom;
    }
}
