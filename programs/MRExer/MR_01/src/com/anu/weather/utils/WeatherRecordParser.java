package com.anu.weather.utils;

public class WeatherRecordParser {
	
	private String stationId;
	private int year;
	private int temperature;
	private boolean valid;
	
	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature = temperature;
	}

	public boolean isValid() {
		return valid;
	}

	public void setValid(boolean valid) {
		this.valid = valid;
	}

	/**
	 * 87-92  +9999 ���Ϸ�
	 */
	
	public void parser(String line) {
		
		//����С��93���Ϸ�
		if(line.length()<93) {
			//valid = false;
			return;
		}
		
		//������Ч
		if(line.substring(87, 92).equals("+9999")){
			//valid = false;
			return;
		}
		
		//�Ϸ����
		if(line.substring(92,93).matches("[01459]")) {
			valid = true;
			stationId = line.substring(0,15);
			year = Integer.parseInt(line.substring(15,19));
			temperature = Integer.parseInt(line.substring(87,92));
		}
		
		
	}

}
