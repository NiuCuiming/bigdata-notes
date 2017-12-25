package com.anu.libimseti.utils;

import java.util.List;

public class LibimsetiParser {

    private String userID;
    private String profileID;
    private Integer rating;
    private boolean valid;

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getProfileID() {
        return profileID;
    }

    public void setProfileID(String profileID) {
        this.profileID = profileID;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }

    public void parser(String line) {

        String[] split = line.split(",");

        if(split.length == 3) {

            userID = split[0].trim();
            profileID = split[1].trim();
            rating = Integer.parseInt(split[2].trim());
            valid = true;
        }
    }

}
