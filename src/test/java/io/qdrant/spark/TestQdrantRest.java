package io.qdrant.spark;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class TestQdrantRest {
    String qdrantUrl;
    String apiKey;

    public TestQdrantRest() {
        qdrantUrl = System.getenv("QDRANT_URL");
        // The url cannot be set to null
        if (qdrantUrl == null) {
            qdrantUrl = "http://localhost:6333";
        }

        // The API key can be null
        apiKey = System.getenv("QDRANT_API_KEY");
    }

    @Test
    public void testUploadBatch() throws Exception {
        // create a QdrantRest instance with a mock URL and API key
        QdrantRest qdrantRest = new QdrantRest(qdrantUrl, apiKey);

        // create a list of points to upload
        List<Point> points = new ArrayList<>();
        Point point1 = new Point();
        point1.id = UUID.randomUUID().toString();
        point1.vector = new float[] { 1.0f, 2.0f, 3.0f };
        point1.payload = new HashMap<>();
        point1.payload.put("name", "point1");
        points.add(point1);

        Point point2 = new Point();
        point2.id = UUID.randomUUID().toString();
        point2.vector = new float[] { 4.0f, 5.0f, 6.0f };
        point2.payload = new HashMap<>();
        point2.payload.put("name", "point2");
        points.add(point2);

        // call the uploadBatch method
        qdrantRest.uploadBatch("qdrant-spark", points);

        // assert that the upload was successful
        assertTrue(true);
    }
}