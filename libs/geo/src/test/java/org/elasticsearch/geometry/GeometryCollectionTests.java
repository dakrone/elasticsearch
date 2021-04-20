/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry;

import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;

public class GeometryCollectionTests extends BaseGeometryTestCase<GeometryCollection<Geometry>> {
    @Override
    protected GeometryCollection<Geometry> createTestInstance(boolean hasAlt) {
        return GeometryTestUtils.randomGeometryCollection(hasAlt);
    }

    public void testBasicSerialization() throws IOException, ParseException {
        WellKnownText wkt = new WellKnownText(true, new GeographyValidator(true));
        assertEquals("GEOMETRYCOLLECTION (POINT (20.0 10.0),POINT EMPTY)",
            wkt.toWKT(new GeometryCollection<Geometry>(Arrays.asList(new Point(20, 10), Point.EMPTY))));

        assertEquals(new GeometryCollection<Geometry>(Arrays.asList(new Point(20, 10), Point.EMPTY)),
            wkt.fromWKT("GEOMETRYCOLLECTION (POINT (20.0 10.0),POINT EMPTY)"));

        assertEquals("GEOMETRYCOLLECTION EMPTY", wkt.toWKT(GeometryCollection.EMPTY));
        assertEquals(GeometryCollection.EMPTY, wkt.fromWKT("GEOMETRYCOLLECTION EMPTY)"));
    }

    @SuppressWarnings("ConstantConditions")
    public void testInitValidation() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(Collections.emptyList()));
        assertEquals("the list of shapes cannot be null or empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(null));
        assertEquals("the list of shapes cannot be null or empty", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new GeometryCollection<>(
            Arrays.asList(new Point(20, 10), new Point(20, 10, 30))));
        assertEquals("all elements of the collection should have the same number of dimension", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> new StandardValidator(false).validate(
            new GeometryCollection<Geometry>(Collections.singletonList(new Point(20, 10, 30)))));
        assertEquals("found Z value [30.0] but [ignore_z_value] parameter is [false]", ex.getMessage());

        new StandardValidator(true).validate(new GeometryCollection<Geometry>(Collections.singletonList(new Point(20, 10, 30))));
    }
}
