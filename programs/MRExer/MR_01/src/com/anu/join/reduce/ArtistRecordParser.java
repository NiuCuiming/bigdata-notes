package com.anu.join.reduce;

import org.apache.hadoop.io.Text;

public class ArtistRecordParser {

    private String artistId;
    private String date;
    private int count;
    private boolean valid = false;

    public void parser(String line) {

        String[] tokens = line.split(",");
        if(tokens != null & tokens.length == 3) {
            artistId = tokens[0].trim();
            date = tokens[1].trim();
            count = Integer.parseInt(tokens[2].trim());
            valid = true;
        }
    }

    public void parser(Text line) {
        parser(line.toString());
    }

    public String getArtistId() {
        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }
}
