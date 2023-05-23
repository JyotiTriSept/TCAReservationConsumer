package com.example.demo.filter;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;

import com.example.demo.adls.UploadReservationDataToADLS;
import com.example.demo.consumer.ConsumerFromReservationEhub;

public class FilterReservationDataForHotel {
	
	@Autowired
	UploadReservationDataToADLS upload;
	
	public void getReservationDataForGivenHotel(String brandCode, String hotelCode) {
		for(String eventData: ConsumerFromReservationEhub.dataList) {
			JSONParser parser = new JSONParser();
			try {
				JSONObject parsedObject = (JSONObject) parser.parse(eventData);
				if(hotelCode.equals((String) parsedObject.get("hotel_code"))
						&& brandCode.equals((String) parsedObject.get("brand_code"))){
					upload.uploadToADLS(ConsumerFromReservationEhub.dataList);
				}
						
						
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
