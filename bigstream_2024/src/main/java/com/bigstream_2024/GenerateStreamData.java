package com.bigstream_2024;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class GenerateStreamData {

    public static void generateRandomStreamData(int eventsCount) {
        Random random = new Random();
        Map<Integer, String> videoIdToGenre = new HashMap<>();
        Map<Integer, String> videoIdToType = new HashMap<>();
        Map<String, Integer> userVideoPairToTimestamp = new HashMap<>();
        Map<String, String> userVideoPairToLastAction = new HashMap<>();
        Map<Integer, Long> userIdToLastGlobalTimestamp = new HashMap<>();

        for (int i = 0; i < eventsCount; i++) {
            int userID = random.nextInt(2);  // Simulate with 2 users
            int videoID = random.nextInt(4); // Simulate with 10 videos
            String userVideoKey = userID + "-" + videoID;

            long globalTimestamp = System.currentTimeMillis() / 1000L;

            // If the last action was "pause", the timestamp should not change
            String lastAction = userVideoPairToLastAction.getOrDefault(userVideoKey, "ends");
            int previousTimestampInVideo = userVideoPairToTimestamp.getOrDefault(userVideoKey, 0);
            int timestampInVideo = previousTimestampInVideo;

            if (!lastAction.equals("pauses")) {
                // Introduce more realistic time variations (1 to 5 seconds)
                int skipDirection = random.nextInt(3) - 1; // -1 (backward), 0 (no skip), 1 (forward)
                int skipAmount = (random.nextInt(5) + 1) * (random.nextBoolean() ? 1 : 10); // Skip 1 to 5 or 10 to 50 seconds
                timestampInVideo = previousTimestampInVideo + (skipDirection * skipAmount);

                // Ensure the timestamp does not go below zero or unrealistically high
                if (timestampInVideo < 0) {
                    timestampInVideo = 0;
                } else if (timestampInVideo > 3600) { // Assuming videos are up to an hour long
                    timestampInVideo = 3600;
                }
            }

            String action = null;

            // Ensure sensible action sequence, including "start" only after "end"
            if (lastAction.equals("ends")) {
                action = "starts";
                timestampInVideo = 0; // Reset timestamp for a new watch
            } else if (lastAction.equals("starts")) {
                action = random.nextBoolean() ? "pauses" : "ends";
            } else if (lastAction.equals("pauses")) {
                action = "plays"; // Can only "plays" after "pauses"
            } else if (lastAction.equals("plays")) {
                action = random.nextBoolean() ? "pauses" : "ends";
            }

            // Ensure video end eventually
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

            // Ensure realistic timing
            if (userIdToLastGlobalTimestamp.containsKey(userID) && globalTimestamp - userIdToLastGlobalTimestamp.get(userID) < 10) {
                globalTimestamp = userIdToLastGlobalTimestamp.get(userID) + 10; // Ensure at least 10 seconds between actions
            }

            System.out.println("UserID: " + userID + ", VideoID: " + videoID + ", GlobalTimestamp: " + globalTimestamp
                    + ", TimestampInVideo: " + timestampInVideo + ", Action: " + action + ", VideoType: " + videoType
                    + ", Genre: " + genre);
        }
    }
}