package com.dremio.gis.geom_utils;

import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.geotools.geometry.GeometryBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import java.nio.ByteBuffer;

public class BinaryGeomUtils {

    public Geometry getGeometry(org.apache.arrow.vector.holders.VarBinaryHolder binary_geom){
        Geometry geom;

        try{
            geom = new WKBReader().read(binary_geom.buffer.nioBuffer(binary_geom.start,
                    binary_geom.end - binary_geom.start).array());
        }catch (ParseException e){
            geom = new GeometryFactory().createPoint(new Coordinate(-9999,-9999));
        }
        return geom;
    }

    public java.nio.ByteBuffer getBinary(Geometry geom){
        return ByteBuffer.wrap(new WKBWriter().write(geom));
    }
}
