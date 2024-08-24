package com.bigstream_2024;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GenerateStreamData {

    public static void generateRandomStreamData(int eventsCount) throws InterruptedException {
        Random random = new Random();
        Map<Integer, String> videoIdToGenre = new HashMap<>();
        Map<Integer, String> videoIdToType = new HashMap<>();
        Map<String, Integer> userVideoPairToTimestamp = new HashMap<>();
        Map<String, String> userVideoPairToLastAction = new HashMap<>();
        Map<Integer, Long> userIdToLastGlobalTimestamp = new HashMap<>();
        Gson gson = new Gson();

        for (int i = 0; i < eventsCount; i++) {
            int userID = random.nextInt(2);  // Simulate with 2 users
            int videoID = random.nextInt(4); // Simulate with 4 videos
            String userVideoKey = userID + "-" + videoID;

            long globalTimestamp = System.currentTimeMillis() / 1000L;

            // If the last action was "pause", the timestamp should not change
            String lastAction = userVideoPairToLastAction.getOrDefault(userVideoKey, "ends");
            int previousTimestampInVideo = userVideoPairToTimestamp.getOrDefault(userVideoKey, 0);
            int timestampInVideo = previousTimestampInVideo;

            if (!lastAction.equals("pauses")) {
                int skipDirection = random.nextInt(3) - 1; // -1 (backward), 0 (no skip), 1 (forward)
                int skipAmount = (random.nextInt(5) + 1) * (random.nextBoolean() ? 1 : 10); // Skip 1 to 5 or 10 to 50 seconds
                timestampInVideo = previousTimestampInVideo + (skipDirection * skipAmount);

                if (timestampInVideo < 0) {
                    timestampInVideo = 0;
                } else if (timestampInVideo > 3600) { // Assuming videos are up to an hour long
                    timestampInVideo = 3600;
                }
            }

            String action = null;

            if (lastAction.equals("ends")) {
                action = "starts";
                timestampInVideo = 0; // Reset timestamp for a new watch
            } else if (lastAction.equals("starts")) {
                action = random.nextBoolean() ? "pauses" : "ends";
            } else if (lastAction.equals("pauses")) {
                action = "plays";
            } else if (lastAction.equals("plays")) {
                action = random.nextBoolean() ? "pauses" : "ends";
            }

            if (action.equals("plays") && random.nextInt(10) > 7) {
                action = "ends";
            }

            userVideoPairToLastAction.put(userVideoKey, action);
            userVideoPairToTimestamp.put(userVideoKey, timestampInVideo);
            userIdToLastGlobalTimestamp.put(userID, globalTimestamp);

            String videoType;
            if (videoIdToType.containsKey(videoID)) {
                videoType = videoIdToType.get(videoID);
            } else {
                String[] videoTypes = {"recording", "livestream"};
                videoType = videoTypes[random.nextInt(videoTypes.length)];
                videoIdToType.put(videoID, videoType);
            }

            String genre;
            if (videoIdToGenre.containsKey(videoID)) {
                genre = videoIdToGenre.get(videoID);
            } else {
                String[] genres = {"Action", "Comedy", "Drama", "Horror", "Sci-Fi", "Documentary"};
                genre = genres[random.nextInt(genres.length)];
                videoIdToGenre.put(videoID, genre);
            }

            if (userIdToLastGlobalTimestamp.containsKey(userID) && globalTimestamp - userIdToLastGlobalTimestamp.get(userID) < 10) {
                globalTimestamp = userIdToLastGlobalTimestamp.get(userID) + 10;
            }

            // Create a StreamEvent object
            StreamEvent event = new StreamEvent(userID, videoID, globalTimestamp, timestampInVideo,
                                                action, videoType, genre);

      
            String jsonRecord = gson.toJson(event);

            // Send JSON record to Kafka
            TestKafkaProducer.sendDataToKafka("sample", jsonRecord);
            Thread.sleep(1000);

            
            System.out.println(jsonRecord);
        }
    }
}
