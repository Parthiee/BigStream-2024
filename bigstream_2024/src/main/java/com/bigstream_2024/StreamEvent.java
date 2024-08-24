package com.bigstream_2024;

public class StreamEvent {
    private int userID;
    private int videoID;
    private long globalTimestamp;
    private int timestampInVideo;
    private String action;
    private String videoType;
    private String genre;

    public StreamEvent(int userID, int videoID, long globalTimestamp, int timestampInVideo,
                       String action, String videoType, String genre) {
        this.userID = userID;
        this.videoID = videoID;
        this.globalTimestamp = globalTimestamp;
        this.timestampInVideo = timestampInVideo;
        this.action = action;
        this.videoType = videoType;
        this.genre = genre;
    }

   
}
