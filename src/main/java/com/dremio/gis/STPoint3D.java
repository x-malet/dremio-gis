package com.dremio.gis;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.memory.ArrowBuf;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;

import javax.inject.Inject;
import java.nio.ByteBuffer;

@FunctionTemplate(name = "st_point3d", scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STPoint3D implements SimpleFunction {
    @Param
    org.apache.arrow.vector.holders.Float8Holder lonParam;

    @Param
    org.apache.arrow.vector.holders.Float8Holder latParam;

    @Param
    org.apache.arrow.vector.holders.Float8Holder elevParam;

    @Output
    org.apache.arrow.vector.holders.VarBinaryHolder out;

    @Inject
    ArrowBuf buffer;

    public void setup() {
    }

    public void eval() {

        double lon = lonParam.value;
        double lat = latParam.value;
        double elevation = elevParam.value;
        Coordinate coord = new CoordinateXY(lon, lat);
        coord.setZ(elevation);
        Geometry point = new GeometryFactory().createPoint(coord);

        java.nio.ByteBuffer pointBytes = ByteBuffer.wrap(new WKBWriter().write(point));
        out.buffer = buffer;
        out.start = 0;
        out.end = pointBytes.remaining();
        buffer.setBytes(0, pointBytes);
    }
}
