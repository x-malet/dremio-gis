/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.gis;

import javax.inject.Inject;

import com.dremio.exec.expr.SimpleFunction;
import com.dremio.exec.expr.annotations.FunctionTemplate;
import com.dremio.exec.expr.annotations.Output;
import com.dremio.exec.expr.annotations.Param;
import org.apache.arrow.memory.ArrowBuf;

@FunctionTemplate(name = "st_geomfromtext", scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STGeomFromTextSrid implements SimpleFunction {
  @Param
  org.apache.arrow.vector.holders.NullableVarCharHolder input;

  @Param
  org.apache.arrow.vector.holders.NullableIntHolder sridParam;

  @Output
  org.apache.arrow.vector.holders.VarBinaryHolder out;

  @Inject
  ArrowBuf buffer;

  public void setup() {
  }

  public void eval() {
    int srid = sridParam.value;
    String wktText = StringFunctionHelpers.toStringFromUTF8(input.start, input.end,
        input.buffer);

    com.esri.core.geometry.ogc.OGCGeometry geom;

    geom = com.esri.core.geometry.ogc.OGCGeometry.fromText(wktText);
    geom.setSpatialReference(com.esri.core.geometry.SpatialReference.create(srid));

    java.nio.ByteBuffer pointBytes = geom.asBinary();
    
    int outputSize = pointBytes.remaining();
    buffer = out.buffer = buffer.reallocIfNeeded(outputSize);
    out.start = 0;
    out.end = outputSize;
    buffer.setBytes(0, pointBytes);
  }
}
