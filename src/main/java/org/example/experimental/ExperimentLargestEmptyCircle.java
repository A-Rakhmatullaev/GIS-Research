package org.example.experimental;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialOperator.KNNQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.example.Main;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ExperimentLargestEmptyCircle {
    String shapefileInputLocation = Main.resourceDir + "/nuclear";

    void start(SparkSession sedona) {
        TempLogger.log(shapefileInputLocation);

        try {
            GeometryFactory geometryFactory = new GeometryFactory();

            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    shapefileInputLocation
            );

            // Flip coordinates to make them in (Latitude, Longitude) format
            spatialRDD.flipCoordinates();

            Dataset<Row> geometryDf = Adapter.toDf(spatialRDD, sedona);
            geometryDf.createTempView("geometryTable");

            Dataset<Row> aggregatedDf = sedona.sql("SELECT ST_Union_Aggr(geometryTable.geometry) AS agg FROM geometryTable");
            aggregatedDf.createTempView("aggregatedTable");

            Dataset<Row> convexHullDf = sedona
                    .sql("SELECT ST_ConvexHull(aggregatedTable.agg) AS convex_hull_geom FROM aggregatedTable")
                    .dropDuplicates();

            Geometry convexHull = convexHullDf.selectExpr("convex_hull_geom")
                    .map((MapFunction<Row, Geometry>) row -> (Geometry) row.get(0), Encoders.kryo(Geometry.class)).first();

            System.out.println("Envelope: " + convexHull.getEnvelopeInternal());

            // Add Convex Hull vertices to the list of all points to form all points within which LEC is calculated
            ArrayList <Geometry> convexHullVertices = new ArrayList<>();
            for (Coordinate coordinate : convexHull.getCoordinates()) {
                convexHullVertices.add(geometryFactory.createPoint(coordinate));
            }
            List <Geometry> allPoints = new ArrayList<>(spatialRDD.rawSpatialRDD.collect());
            System.out.println("Prev"+allPoints.size());
            allPoints.addAll(convexHullVertices);
            System.out.println("Aft: " + allPoints.size());
            Dataset<Geometry> allPointsDataset = sedona.createDataset(allPoints, Encoders.kryo(Geometry.class));
            spatialRDD.setRawSpatialRDD(allPointsDataset.toJavaRDD());

//            List<Point> points = new ArrayList<>();
//            GeometryFactory geometryFactory = new GeometryFactory();
//            points.add(geometryFactory.createPoint(new Coordinate(1.0, 1.0)));
//            points.add(geometryFactory.createPoint(new Coordinate(2.0, 2.0)));
//            points.add(geometryFactory.createPoint(new Coordinate(3.0, 3.0)));
//
//            Coordinate[] coordinates = new Coordinate[6];
//            coordinates[0] = new Coordinate(1.0, 1.0);
//            coordinates[1] = new Coordinate(2.0, 2.0);
//            coordinates[2] = new Coordinate(1.0, 3.0);
//            coordinates[3] = new Coordinate(1.5, 6.0);
//            coordinates[4] = new Coordinate(3.0, 2.0);
//            coordinates[5] = new Coordinate(7.0, 2.0);
            // Add more points as needed

//            JavaRDD<Point> pointRDD = sedona.sparkContext().parallelize(points);
//            PointRDD pointSpatialRDD = new PointRDD(pointRDD);

            //spatialRDD.rawSpatialRDD.map((MapFunction<Geometry, Coordinate>) point -> (Coordinate) point.getCoordinate(), Encoders.kryo(Coordinate.class));

            List<Coordinate> coordinateList = spatialRDD.rawSpatialRDD.collect().stream().map(Geometry::getCoordinate).collect(Collectors.toList());
            Coordinate[] coordinates = new Coordinate[coordinateList.size()];
            coordinateList.toArray(coordinates);
            VoronoiDiagramBuilder voronoiDiagramBuilder = new VoronoiDiagramBuilder();
            voronoiDiagramBuilder.setSites(Arrays.asList(coordinates));
            voronoiDiagramBuilder.setClipEnvelope(convexHull.getEnvelopeInternal());

            GeometryCollection geometryCollection = (GeometryCollection) voronoiDiagramBuilder.getDiagram(geometryFactory);
            List<Polygon> voronoiPolygons = new ArrayList<>();
            for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
                voronoiPolygons.add((Polygon) geometryCollection.getGeometryN(i));
            }

//            // Find Voronoi vertices
//            List<Coordinate> voronoiVertices = new ArrayList<>();
//            for (Polygon polygon : voronoiPolygons) {
//                voronoiVertices.addAll(List.of(polygon.getCoordinates()));
//            }
//
//            System.out.println("Total # of Voronoi vertices: " + voronoiVertices.size());
//            System.out.println("Distinct # of Voronoi vertices: " + voronoiVertices.stream().distinct().count());
//
//            // Transform each Voronoi vertex from Coordinate to Geometry
//            List<Geometry> vertices = voronoiVertices.stream().map(geometryFactory::createPoint).collect(Collectors.toList());

            // Obtain intersection points of Voronoi polygons' edges and Convex Hull + all other Voronoi vertices
            // This also ensures that all these vertices (points) are within Convex Hull
            List<Geometry> allVoronoiPolygonsAfterIntersectionWithConvexHull = new ArrayList<>();
            for (Polygon voronoiPolygon : voronoiPolygons) {
                for (int i = 0; i < voronoiPolygon.getNumGeometries(); i++) {
                    Geometry voronoiEdge = voronoiPolygon.getGeometryN(i);
                    for (int j = 0; j < convexHull.getNumGeometries(); j++) {
                        Geometry hullEdge = convexHull.getGeometryN(j);
                        Geometry intersection = voronoiEdge.intersection(hullEdge);
                        if (!intersection.isEmpty()) {
                            allVoronoiPolygonsAfterIntersectionWithConvexHull.add(intersection);
                        }
                    }
                }
            }

            // Find All LEC Centers/Vertices
            List<Coordinate> lecCenters = new ArrayList<>();
            for (Geometry polygon : allVoronoiPolygonsAfterIntersectionWithConvexHull) {
                lecCenters.addAll(List.of(polygon.getCoordinates()));
            }

            System.out.println("Total # of Voronoi vertices: " + lecCenters.size());
            System.out.println("Distinct # of Voronoi vertices: " + lecCenters.stream().distinct().count());

            // Transform each LEC center/vertex from Coordinate to Geometry
            List<Geometry> lecCentersGeom = lecCenters.stream().map(geometryFactory::createPoint).collect(Collectors.toList());


//            System.out.println("V: " + intersectionPoints);
//            intersectionPoints.forEach(System.out::println);
//            Dataset<Geometry> p = sedona.createDataset(intersectionPoints, Encoders.kryo(Geometry.class));
//            SpatialRDD <Geometry> some = new SpatialRDD<>();
//            some.setRawSpatialRDD(p.toJavaRDD());
//            some.flipCoordinates();
//            some.saveAsGeoJSON(shapefileInputLocation + "/output/inter_result.json");

            AtomicDouble maxRadius = new AtomicDouble(0);
            AtomicDouble maxRadius2 = new AtomicDouble(0);
            AtomicDouble maxRadius3 = new AtomicDouble(0);
            AtomicDouble maxRadius4 = new AtomicDouble(0);
            AtomicDouble maxRadius5 = new AtomicDouble(0);
            AtomicReference <Geometry> lecCenter = new AtomicReference<>();
            AtomicReference <Geometry> lecCenter2 = new AtomicReference<>();
            AtomicReference <Geometry> lecCenter3 = new AtomicReference<>();
            AtomicReference <Geometry> lecCenter4 = new AtomicReference<>();
            AtomicReference <Geometry> lecCenter5 = new AtomicReference<>();

            lecCentersGeom.parallelStream().distinct().forEach(vertex -> {
                List<Geometry> nearestPoints = KNNQuery.SpatialKnnQuery(spatialRDD, vertex, 1, false);
                Geometry nearestPoint = nearestPoints.get(0);
                double distance = vertex.distance(nearestPoint);
                if (distance > maxRadius.get()) {
                    if(distance > maxRadius2.get()) {
                        if(distance > maxRadius3.get()) {
                            if(distance > maxRadius4.get()) {
                                if(distance > maxRadius5.get()) {
                                    maxRadius5.set(distance);
                                    lecCenter5.set(vertex);
                                } else {
                                    maxRadius4.set(distance);
                                    lecCenter4.set(vertex);
                                }
                            } else {
                                maxRadius3.set(distance);
                                lecCenter3.set(vertex);
                            }
                        } else {
                            maxRadius2.set(distance);
                            lecCenter2.set(vertex);
                        }
                    } else {
                        maxRadius.set(distance);
                        lecCenter.set(vertex);
                    }
                }
            });

            //! vertices.distinct = vertices +
            //! intersection points of voronoi edges and convex hull + vertices = allVertices +
            //! spatialRdd.add(all_points + convexHullVertices) +

            System.out.println("Largest Empty Circle Center: " + lecCenter.get());
            System.out.println("Largest Empty Circle Radius: " + maxRadius.get());

            System.out.println("Largest Empty Circle Center 2: " + lecCenter2.get());
            System.out.println("Largest Empty Circle Radius 2: " + maxRadius2.get());

            System.out.println("Largest Empty Circle Center 3: " + lecCenter3.get());
            System.out.println("Largest Empty Circle Radius 3: " + maxRadius3.get());

            System.out.println("Largest Empty Circle Center 4: " + lecCenter4.get());
            System.out.println("Largest Empty Circle Radius 4: " + maxRadius4.get());

            System.out.println("Largest Empty Circle Center 5: " + lecCenter5.get());
            System.out.println("Largest Empty Circle Radius 5: " + maxRadius5.get());

            spatialRDD.flipCoordinates();
            spatialRDD.saveAsGeoJSON(shapefileInputLocation + "/output/all_points_result.json");

            // Create the largest empty circle
            Geometry lecCircle = geometryFactory.createPoint(lecCenter.get().getCoordinate()).buffer(maxRadius.get());
            Geometry lecCircle2 = geometryFactory.createPoint(lecCenter2.get().getCoordinate()).buffer(maxRadius2.get());
            Geometry lecCircle3 = geometryFactory.createPoint(lecCenter3.get().getCoordinate()).buffer(maxRadius3.get());
            Geometry lecCircle4 = geometryFactory.createPoint(lecCenter4.get().getCoordinate()).buffer(maxRadius4.get());
            Geometry lecCircle5 = geometryFactory.createPoint(lecCenter5.get().getCoordinate()).buffer(maxRadius5.get());
            ArrayList <Geometry> l = new ArrayList<>();
            l.add(lecCircle);
            l.add(lecCircle2);
            l.add(lecCircle3);
            l.add(lecCircle4);
            l.add(lecCircle5);
            Dataset<Geometry> b = sedona.createDataset(l, Encoders.kryo(Geometry.class));
            SpatialRDD <Geometry> last = new SpatialRDD<>();
            last.setRawSpatialRDD(b.toJavaRDD());
            last.flipCoordinates();
            last.saveAsGeoJSON(shapefileInputLocation + "/output/lec_result.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void generateVoronoiGeoJSON(List<Polygon> voronoiPolygons, SparkSession sedona) {
        // Convert Polygons to WKT strings
        WKTWriter wktWriter = new WKTWriter();
        List<String> wktList = new ArrayList<>();
        for (Polygon polygon : voronoiPolygons) {
            wktList.add(wktWriter.write(polygon));
        }

        //Polygon v = voronoiPolygons.get(0);
        Dataset<Polygon> wktDataset = sedona.createDataset(voronoiPolygons, Encoders.kryo(Polygon.class));
        JavaRDD<Geometry> d = wktDataset.map((MapFunction<Polygon, Geometry>) polygon -> (Geometry) polygon, Encoders.kryo(Geometry.class)).toJavaRDD();


        SpatialRDD <Geometry> spatialRDD = new SpatialRDD<>();
        //SpatialRDD <Geometry> s = Adapter.toSpatialRdd(wktDataset.toDF("value"), "voronoi_polygons");
        spatialRDD.setRawSpatialRDD(d);
        spatialRDD.flipCoordinates();
        spatialRDD.saveAsGeoJSON(shapefileInputLocation + "/output/voronoi_result.json");
        //s.flipCoordinates();
        //s.saveAsGeoJSON(shapefileInputLocation + "/output/voronoi_result.json");


        // Convert list of Voronoi polygons to SpatialRDD
//            Dataset<Row> voronoiDf = sedona.createDataFrame(voronoiPolygons, Polygon.class);
//            voronoiDf.show(false);
//            SpatialRDD<Polygon> voronoiSpatialRDD = new SpatialRDD<>();
//            voronoiSpatialRDD.setRawSpatialRDD(voronoiRDD);

        //System.out.println("F: " + voronoiPolygons);
    }
}
