package org.example;

import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;


import java.util.Arrays;

public class ExperimentConvexHull {
    String shapefileInputLocation = Main.resourceDir + "/nuclear";

    void start(SparkSession sedona) {
        TempLogger.log(shapefileInputLocation);

        try {
            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    shapefileInputLocation
            );

            // Flip coordinates to make them in (Latitude, Longitude) format
            spatialRDD.flipCoordinates();

            //spatialRDD.boundaryEnvelope.

            Dataset<Row> tempDf = Adapter.toDf(spatialRDD, sedona);
            tempDf.createTempView("geometryTable");

            //Dataset<Row> s =
//                    sedona.sql(
//                    "SELECT "
//                            + Arrays.toString(tempDf.columns()).replaceAll("^.|.$", "")
//                            + ", ST_ConvexHull(geometryTable.geometry) AS convex_hull_geom FROM geometryTable")
//                    .show((int) tempDf.count(), false);

            System.out.println("Q: " + tempDf.dropDuplicates().count());
            Dataset<Row> v = sedona.sql("SELECT ST_Union_Aggr(geometryTable.geometry) AS ass FROM geometryTable");
            System.out.println("B: " + v.count());
            v.show((int) v.count(), false);

            v.createTempView("vTable");

//            List<Geometry> geometryList = new ArrayList<>();
//            tempDf.javaRDD().collect().forEach(row -> {
//                Geometry geometry = (Geometry) row.getAs("geometry");
//                geometryList.add(geometry);
//            });

            Dataset<Row> s = sedona.sql(
                            "SELECT ST_ConvexHull(vTable.ass) AS convex_hull_geom FROM vTable")
                    .dropDuplicates();

            System.out.println("GG: " + s.count());
            s.show((int) s.count(), false);


            s.createTempView("shapeTable");
//            Dataset<Row> p = sedona.sql("SELECT ST_Point(cast(shapeTable.longitude as Decimal(24,20)),cast(shapeTable.latitude as Decimal(24,20))) as shape FROM shapeTable");
//
//            p.createTempView("boundTable");
//            Dataset<Row> f = sedona.sql("SELECT ST_Envelope_Aggr(shape) as bound FROM boundTable");
//
//            f.createTempView("pointTable");
            GeometryFactory geometryFactory = new GeometryFactory();
            Coordinate[] coordinates = new Coordinate[5];
            coordinates[0] = new Coordinate(-175, 175);
            coordinates[1] = new Coordinate(-175, -175);
            coordinates[2] = new Coordinate(175, 175);
            coordinates[3] = new Coordinate(175, -175);
            coordinates[4] = coordinates[0]; // The last coordinate is the same as the first coordinate in order to compose a closed ring
            Polygon polygonQueryWindow = geometryFactory.createPolygon(coordinates);
            new GeometryFactory(new PrecisionModel(), 4326);
            //Dataset<Row> k = sedona.sql("SELECT pixel, convex_hull_geom FROM shapeTable LATERAL VIEW explode(ST_Pixelize(ST_Transform(convex_hull_geom, 'epsg:4326','epsg:3857'), 256, 256, '%polygonQueryWindow')) AS pixel");
            //Dataset <Row> y = sedona.sql("SELECT convex_hull_geom FROM shapeTableST_Pixelize(ST_Transform(convex_hull_geom, 'epsg:4326','epsg:3857'), 256, 256, '%polygonQueryWindow')");
            //y.show(false);
            //LATERAL VIEW ST_Pixelize(ST_Transform(convex_hull_geom, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundTable)) AS pixel
            //k.show((int) k.count(), false);
//            ImageGenerator imageGenerator = new ImageGenerator();
//            ScatterPlot plot = new ScatterPlot(1000, 600, )
//            imageGenerator.SaveRasterImageAsLocalFile()

            //k.createTempView("pixels");
            //Dataset<Row> m = sedona.sql("SELECT pixel, ST_Colorize(1, 4) AS color FROM pixels");
            //m.show(false);

            //sedona.sql("SELECT ST_Envelope_Aggr(convex_hull_geom) as bound FROM pointtable");
            //System.out.println("FF: " + s.count());
            //System.out.println("FF2: " + tempDf.dropDuplicates("powerplant").count());
            //s.show((int) tempDf.count(), false);
            // Convert the polygon to WKT
            String polygonQueryWindowWKT = polygonQueryWindow.toText();

            System.out.println("Polygon: " + polygonQueryWindowWKT);


            CoordinateReferenceSystem w1 = CRS.decode("EPSG:4326");
            CoordinateReferenceSystem w2 = CRS.decode("EPSG:3857");
            MathTransform transformation = CRS.findMathTransform(w1, w2);
            Dataset <Geometry> p = s.selectExpr("convex_hull_geom")
                    .map((MapFunction<Row, Geometry>) row ->
                            JTS.transform((Geometry) row.get(0), transformation), Encoders.kryo(Geometry.class));
            //p.show((int) p.count(), false);



            StructField[] fields = new StructField[]{new StructField()};
            StructType schema = new StructType(fields);
            Dataset <Row> bound = sedona.createDataFrame(Arrays.asList(polygonQueryWindow.getCoordinates()), Coordinate.class);
            //bound.show(false);
            //bound.createTempView("boundTable");
//            sedona.sql("SELECT ST_Point(cast(boundTable.x as Decimal(24,20)),cast(boundTable.y as Decimal(24,20))) as shape FROM boundTable").createTempView("pointTable");
//            sedona.sql("SELECT ST_Envelope_Aggr(shape) as bound FROM pointTable")
//                    .map((MapFunction<Row, Geometry>) row ->
//                    JTS.transform((Geometry) row.get(0), transformation), Encoders.kryo(Geometry.class)).show(false);

            Dataset <Geometry> tempY = sedona.sql("SELECT ST_Envelope_Aggr(convex_hull_geom) as bound FROM shapeTable")
                    .map((MapFunction<Row, Geometry>) row ->
                            JTS.transform((Geometry) row.get(0), transformation), Encoders.kryo(Geometry.class));

            //tempY.show(false);

            tempY.createTempView("yTable");

            p.createTempView("bTable");

            //Dataset<Row> result = sedona.sql("SELECT ST_Pixelize(bTable.value, 256, 256, (SELECT value FROM yTable)) AS pixel_grid FROM bTable");
            //result.show(false);


            //s.selectExpr("country", "ST_AsGeoJSON(convex_hull_geom) AS geom");
            s.selectExpr("ST_AsGeoJSON(convex_hull_geom) AS geom");
            JavaRDD<Geometry> vv = s.selectExpr("convex_hull_geom").map((MapFunction<Row, Geometry>) row -> (Geometry) row.get(0), Encoders.kryo(Geometry.class)).toJavaRDD();
            spatialRDD.setRawSpatialRDD(vv);
            spatialRDD.flipCoordinates();
            spatialRDD.saveAsGeoJSON(shapefileInputLocation + "/output/result.json");
            //write().json(shapefileInputLocation + "/output/result.json")
            //s.toJSON().show(false);
            //s.write().json(shapefileInputLocation + "/output/result.json");

            // Register the UDF
//            sedona.udf().register("transformGeometry",
//                    (Geometry geom) -> JTS.transform(geom, transformation),
//                    DataTypes.BinaryType);

//            String query = "SELECT ST_Transform(shapeTable.convex_hull_geom, 'EPSG:4326','EPSG:3857') FROM shapeTable";
//            Dataset<Row> result = sedona.sql(query);
//            System.out.println("Count: " + result.dropDuplicates().count());

//            String query = String.format(
//                            "SELECT pixel_grid, convex_hull_geom " +
//                            "FROM shapeTable " +
//                            "LATERAL VIEW explode(ST_Pixelize(" +
//                            "ST_Transform(convex_hull_geom, 'epsg:4326', 'epsg:3857'), " +
//                            "256, " +
//                            "256, " +
//                            "ST_Transform(ST_GeomFromWKT('%s'), 'epsg:4326', 'epsg:3857')" +
//                            ")) AS pixel_grid",
//                    polygonQueryWindowWKT
//            );

//            Dataset<Row> result = sedona.sql(query);
//            try {
//                System.out.println(result.dropDuplicates().count());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }


            //result.show(false);
        } catch (Exception e) {
            System.out.println("Printing errors: "  + e);
            e.printStackTrace();
        }
    }
}
