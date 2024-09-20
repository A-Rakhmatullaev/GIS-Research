package org.example;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.sedona.core.formatMapper.shapefileParser.ShapefileReader;
import org.apache.sedona.core.spatialOperator.KNNQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.sedona.sql.utils.Adapter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.example.utilties.TempLogger;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LargestEmptyCircle {
    String currentSetLocation = Main.resourceDir + "/nuclear";
    String fileInputLocation = currentSetLocation + "/input";
    String fileOutputLocation = currentSetLocation + "/output/lec";

    void start(SparkSession sedona) {
        TempLogger.log(fileInputLocation);

        try {
            GeometryFactory geometryFactory = new GeometryFactory();

            SpatialRDD<Geometry> spatialRDD = ShapefileReader.readToGeometryRDD(
                    JavaSparkContext.fromSparkContext(sedona.sparkContext()),
                    fileInputLocation
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
            allPoints.addAll(convexHullVertices);
            Dataset<Geometry> allPointsDataset = sedona.createDataset(allPoints, Encoders.kryo(Geometry.class));
            spatialRDD.setRawSpatialRDD(allPointsDataset.toJavaRDD());

            // Start creating voronoi polygons
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

            // Uncomment if needed:
            // Find Voronoi vertices
//            List<Coordinate> voronoiVertices = new ArrayList<>();
//            for (Polygon polygon : voronoiPolygons) {
//                voronoiVertices.addAll(List.of(polygon.getCoordinates()));
//            }
//
//            System.out.println("Total # of Voronoi vertices: " + voronoiVertices.size());
//            System.out.println("Distinct # of Voronoi vertices: " + voronoiVertices.stream().distinct().count());
//

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

            // Uncomment if needed - it might be needed when you run python code because then you need Voronoi diagram layer to display, not only circle and points
            // generateVoronoiGeoJSON(allVoronoiPolygonsAfterIntersectionWithConvexHull, sedona);

            // Find All LEC Centers/Vertices
            List<Coordinate> lecCenters = new ArrayList<>();
            for (Geometry polygon : allVoronoiPolygonsAfterIntersectionWithConvexHull) {
                lecCenters.addAll(List.of(polygon.getCoordinates()));
            }

            System.out.println("Total # of Voronoi vertices: " + lecCenters.size());
            System.out.println("Distinct # of Voronoi vertices: " + lecCenters.stream().distinct().count());

            // Transform each LEC center/vertex from Coordinate to Geometry
            List<Geometry> lecCentersGeom = lecCenters.stream().map(geometryFactory::createPoint).collect(Collectors.toList());

            // Find max radius and center of the circle with the largest radius
            AtomicDouble maxRadius = new AtomicDouble(0);
            AtomicReference<Geometry> lecCenter = new AtomicReference<>();

            lecCentersGeom.parallelStream().distinct().forEach(vertex -> {
                List<Geometry> nearestPoints = KNNQuery.SpatialKnnQuery(spatialRDD, vertex, 1, false);
                Geometry nearestPoint = nearestPoints.get(0);
                double distance = vertex.distance(nearestPoint);
                if (distance > maxRadius.get()) {
                    maxRadius.set(distance);
                    lecCenter.set(vertex);
                }
            });

            System.out.println("Largest Empty Circle Center: " + lecCenter.get());
            System.out.println("Largest Empty Circle Radius: " + maxRadius.get());

            spatialRDD.flipCoordinates();
            // Save results - all points
            spatialRDD.saveAsGeoJSON(fileOutputLocation + "/all_points_result.json");

            // Create the largest empty circle for the dataset
            Geometry lecCircle = geometryFactory.createPoint(lecCenter.get().getCoordinate()).buffer(maxRadius.get());
            ArrayList <Geometry> l = new ArrayList<>();
            l.add(lecCircle);
            Dataset<Geometry> b = sedona.createDataset(l, Encoders.kryo(Geometry.class));
            SpatialRDD <Geometry> result = new SpatialRDD<>();
            result.setRawSpatialRDD(b.toJavaRDD());
            result.flipCoordinates();

            // Save results - LEC
            result.saveAsGeoJSON(fileOutputLocation + "/lec_result.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void generateVoronoiGeoJSON(List<Geometry> voronoiPolygons, SparkSession sedona) {
        // Convert Polygons to WKT strings
        WKTWriter wktWriter = new WKTWriter();
        List<String> wktList = new ArrayList<>();
        for (Geometry polygon : voronoiPolygons) {
            wktList.add(wktWriter.write(polygon));
        }
        Dataset<Geometry> wktDataset = sedona.createDataset(voronoiPolygons, Encoders.kryo(Geometry.class));

        SpatialRDD <Geometry> spatialRDD = new SpatialRDD<>();
        spatialRDD.setRawSpatialRDD(wktDataset.toJavaRDD());
        spatialRDD.flipCoordinates();
        // Save results - Voronoi Diagram
        spatialRDD.saveAsGeoJSON(fileOutputLocation + "/voronoi_diagram.json");
    }
}
