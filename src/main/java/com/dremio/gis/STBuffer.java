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
import com.dremio.gis.geom_utils.BinaryGeomUtils;
import org.apache.arrow.memory.ArrowBuf;
import org.locationtech.jts.geom.Geometry;

@FunctionTemplate(name = "st_buffer", scope = FunctionTemplate.FunctionScope.SIMPLE,
  nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
public class STBuffer extends BinaryGeomUtils implements SimpleFunction {
  @Param
  org.apache.arrow.vector.holders.VarBinaryHolder geom1Param;

  @Param(constant = true)
  org.apache.arrow.vector.holders.Float8Holder bufferRadiusParam;

  @Output
  org.apache.arrow.vector.holders.VarBinaryHolder out;

  @Inject
  ArrowBuf buffer;

  public void setup() {
  }

  public void eval() {
    double bufferRadius = bufferRadiusParam.value;
    Geometry geom = this.getGeometry(geom1Param);

    Geometry bufferedGeom = geom.buffer(bufferRadius);

    java.nio.ByteBuffer bufferedGeomBytes = this.getBinary(bufferedGeom);

    int outputSize = bufferedGeomBytes.remaining();
    buffer = out.buffer = buffer.reallocIfNeeded(outputSize);
    out.start = 0;
    out.end = outputSize;
    buffer.setBytes(0, bufferedGeomBytes);
  }
}
