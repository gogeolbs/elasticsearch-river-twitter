package org.elasticsearch.river.twitter.utils;

import java.io.IOException;
import java.io.StringWriter;

import org.geotools.geojson.geom.GeometryJSON;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.WKBWriter;

public class SpatialUtils {
    private static final byte[] EMPTYGEOM = new byte[] { 0 };
    public static int WGS84_srid = 4326;
    private static PrecisionModel PRECISION_MODEL = new PrecisionModel(
            PrecisionModel.FLOATING);
    private static GeometryFactory gf = new GeometryFactory(PRECISION_MODEL,
            WGS84_srid);
    
    public static Point createPoint(Coordinate coordinate) {
        return gf.createPoint(coordinate);
    }

    public static Point createPoint(double latitude, double longitude) {
        return createPoint(new Coordinate(longitude, latitude));
    }
    
    public static String toJson(Geometry geom) throws IOException {
        GeometryJSON gjson = new GeometryJSON(
                PRECISION_MODEL.getMaximumSignificantDigits());
        StringWriter writer = new StringWriter();
        gjson.write(geom, writer);
        return writer.toString();
    }
    
    public static byte[] serialize(Geometry geom) {
        WKBWriter wkbw = new WKBWriter();

        if (geom.isEmpty())
            return EMPTYGEOM;

        return wkbw.write(geom);
    }
}
