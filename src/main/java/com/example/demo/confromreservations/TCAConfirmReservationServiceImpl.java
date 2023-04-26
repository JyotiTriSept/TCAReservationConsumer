package com.example.demo.confromreservations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import com.example.demo.model.ConfirmReservationModel;
import com.example.demo.model.ReservationCodeModel;



@Service
public class TCAConfirmReservationServiceImpl implements TCAConfirmReservationsService {

	@Override
	@Async("consumerAsyncExecutor1")
	public Future<Void> getConfirmReservations(String accessToken, String data) throws Exception {
	
			JSONObject jsonObject;
			try {
				jsonObject = (JSONObject) new JSONParser().parse(data);

				// to get pms_code from reservation data
				String reservationDatList = (String) jsonObject.get("ReservationData");
				List<ReservationCodeModel> reservation_codes = new ArrayList<ReservationCodeModel>();
				List<String> collect = reservationDatList.lines().collect(Collectors.toList());
				collect.forEach(r -> {
					JSONObject parsedJSONObj;
					try {
						parsedJSONObj = (JSONObject) new JSONParser().parse(r);
						ReservationCodeModel resCode = new ReservationCodeModel((String) parsedJSONObj.get("pms_code"),
								(String) parsedJSONObj.get("pms_code"));
						reservation_codes.add(resCode);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				});

				ConfirmReservationModel confrimReq = new ConfirmReservationModel((String) jsonObject.get("group_code"),
						(String) jsonObject.get("brand_code"), (String) jsonObject.get("hotel_code"));
				confrimReq.setReservation_codes(reservation_codes);

				// api req
				/*
				 * ResponseEntity<String> responseEntity = webClient.post()
				 * .uri("/ISL/ConfirmReservations") .header("Authorization", "Bearer " +
				 * accessToken) .accept(MediaType.APPLICATION_JSON, MediaType.ALL)
				 * .contentType(MediaType.APPLICATION_JSON) .bodyValue(confrimReq) .retrieve()
				 * .onStatus(status -> (status.value() == 204 || status.value() == 500),
				 * clientResponse -> Mono.empty()) .toEntity(String.class) .block();
				 * 
				 * if(responseEntity.getStatusCodeValue() == 200) {
				 * System.out.println("Reservations Confirmed!!"); } else {
				 * System.out.print("Reservations confirmation failed"); }
				 */

			} catch (ParseException e) {

			}
			return new AsyncResult<Void>(null);

	}

	@Override
	public void storeExceptionIntoBlob(Exception e) {
		//StoreReservExceptionToBlob.updateToLatestFolder(e.toString());
		//StoreReservExceptionToBlob.storingExceptionInArchiveLocation(e.toString(),"ConfirmReservations");	

	}

}
