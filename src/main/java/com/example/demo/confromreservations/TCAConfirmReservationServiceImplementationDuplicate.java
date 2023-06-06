package com.example.demo.confromreservations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import com.example.demo.exception.StoreReservExceptionToBlob;
import com.example.demo.model.ConfirmReservationModel;
import com.example.demo.model.ReservationCodeModel;

import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
public class TCAConfirmReservationServiceImplementationDuplicate {
	private static final String GROUPCODE ="group_code";
	private static final String BRANDCODE ="brand_code";
	private static final String HOTELCODE ="hotel_code";
	private static final String RESERVATIONDATA ="ReservationData";
	private static String fileName;
	
	@Async("consumerAsyncExecutor1")
	public Future<Void> getConfirmReservations(String accessToken, String data ) throws Exception {
		WebClient webClient = WebClient.builder().baseUrl("https://cs-lab.amr.innsist.tca-ss.com/api").build();
			JSONObject jsonObject;
			try {
				jsonObject = (JSONObject) new JSONParser().parse(data);

				// to get pms_code from reservation data
				String reservationDataListString = (String) jsonObject.get(RESERVATIONDATA);
				JSONArray  reservationDataList= (JSONArray) new JSONParser().parse(reservationDataListString);
				List<ReservationCodeModel> reservation_codes = new ArrayList<ReservationCodeModel>();
				//for(int i=0; i< reservationDataList.size();i++) {
					for(int i=0; i<1;i++) {
					JSONObject parsedJSONObj = (JSONObject) reservationDataList.get(i);
					ReservationCodeModel resCode = new ReservationCodeModel((String) parsedJSONObj.get("pms_code"),
							(String) parsedJSONObj.get("pms_code"));
					reservation_codes.add(resCode);
				}
				/*List<String> collect = reservationDataList.lines().collect(Collectors.toList());
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
				});*/

				ConfirmReservationModel confirmReq = new ConfirmReservationModel((String) jsonObject.get(GROUPCODE),
						(String) jsonObject.get(BRANDCODE), (String) jsonObject.get(HOTELCODE));
				confirmReq.setReservation_codes(reservation_codes);
				fileName = "AMR"+"_"+(String) jsonObject.get(BRANDCODE)+"_"+(String) jsonObject.get(HOTELCODE);
				// api req
				  ResponseEntity<String> responseEntity = webClient.post()
				                                          .uri("/ISL/ConfirmReservations") 
				                                          .header("Authorization", "Bearer " +accessToken) 
				                                          .accept(MediaType.APPLICATION_JSON, MediaType.ALL)
				                                          .contentType(MediaType.APPLICATION_JSON) 
				                                          .bodyValue(confirmReq) 
				                                          .retrieve()
				                                          .onStatus(status -> (status.value() == 204 || status.value() == 500),clientResponse -> Mono.empty()) 
				                                          .toEntity(String.class)
				                                          .retryWhen(Retry.backoff(3, Duration.ofSeconds(5)).doAfterRetry(retrySignal -> {
								                                System.out.println("Retried TCA Confirm Reservartions API: " + retrySignal.totalRetries());
							                                  }))
				                                          .block();
				  
				//System.out.println("Reservations Confirmed!!"); 
				  if(responseEntity.getStatusCodeValue() == 200) {
				  } 
				  else {
				  System.out.print("Reservations confirmation failed"); 
				  }
				  return new AsyncResult<Void>(null);

			} catch (ParseException e) {

				System.out.println(e.getLocalizedMessage());
				System.out.println(e.getMessage());
				System.out.println(e.getCause());
				String storedMessage = "Error Message: " + e.getMessage() + "Cause: " + e.getCause() + "Message: "
						+ e.getLocalizedMessage();
				StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementationDuplicate.fileName);
				StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementationDuplicate.fileName);
				return new AsyncResult<Void>(null);
			}catch (Exception e) {
				if (e.getCause() instanceof WebClientResponseException) {

					WebClientResponseException cause = (WebClientResponseException) e.getCause();
					System.out.println(cause.getResponseBodyAsString());
					System.out.println(cause.getMostSpecificCause());
					System.out.println(cause.getMessage());
					String storedMessage = "Error Message: " + cause.getMessage() + "Specific Cause: "
							+ cause.getMostSpecificCause() + "Response Body: " + cause.getResponseBodyAsString();
					StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementationDuplicate.fileName);
					StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementationDuplicate.fileName);
					return new AsyncResult<Void>(null);
					
				} else if (e.getCause() instanceof WebClientRequestException) {
					WebClientRequestException cause = (WebClientRequestException) e.getCause();
					System.out.println(cause.getRootCause());
					System.out.println(cause.getMostSpecificCause());
					System.out.println(cause.getMessage());
					
					String storedMessage = "Error Message: " + cause.getMessage() + "Specific Cause: "
							+ cause.getMostSpecificCause() + "Root Cause: " + cause.getRootCause();
					StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementationDuplicate.fileName);
					StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementationDuplicate.fileName);
					return new AsyncResult<Void>(null);

				} else {

					System.out.println(e.getLocalizedMessage());
					System.out.println(e.getMessage());
					System.out.println(e.getCause());
					String storedMessage = "Error Message: " + e.getMessage() + "Cause: " + e.getCause() + "Message: "
							+ e.getLocalizedMessage();
					StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementationDuplicate.fileName);
					StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementationDuplicate.fileName);
					return new AsyncResult<Void>(null);
				}

			}
			

	}
}
