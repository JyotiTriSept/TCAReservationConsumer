package com.example.demo.confromreservations;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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
public class TCAConfirmReservationServiceImplementation {
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
				String reservationDatList = (String) jsonObject.get(RESERVATIONDATA);
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

				ConfirmReservationModel confrimReq = new ConfirmReservationModel((String) jsonObject.get(GROUPCODE),
						(String) jsonObject.get(BRANDCODE), (String) jsonObject.get(HOTELCODE));
				confrimReq.setReservation_codes(reservation_codes);
				fileName = "AMR"+"_"+(String) jsonObject.get(BRANDCODE)+"_"+(String) jsonObject.get(HOTELCODE);
				// api req
				 /* ResponseEntity<String> responseEntity = webClient.post()
				                                          .uri(keyVaultObject.getConfirmReservationEndpoint()) 
				                                          .header("Authorization", "Bearer " +accessToken) .accept(MediaType.APPLICATION_JSON, MediaType.ALL)
				                                          .contentType(MediaType.APPLICATION_JSON) .bodyValue(confrimReq) .retrieve()
				                                          .onStatus(status -> (status.value() == 204 || status.value() == 500),clientResponse -> Mono.empty()) 
				                                          .toEntity(String.class)
				                                          .retryWhen(Retry.backoff(3, Duration.ofSeconds(5)).doAfterRetry(retrySignal -> {
								                                System.out.println("Retried TCA Login API: " + retrySignal.totalRetries());
							                                  }))
				                                          .block();
				  
				  if(responseEntity.getStatusCodeValue() == 200) {
				  System.out.println("Reservations Confirmed!!"); 
				  } 
				  else {
				  System.out.print("Reservations confirmation failed"); 
				  }*/
				  return new AsyncResult<Void>(null);

			} catch (ParseException e) {

				System.out.println(e.getLocalizedMessage());
				System.out.println(e.getMessage());
				System.out.println(e.getCause());
				String storedMessage = "Error Message: " + e.getMessage() + "Cause: " + e.getCause() + "Message: "
						+ e.getLocalizedMessage();
				StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementation.fileName);
				StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementation.fileName);
				return new AsyncResult<Void>(null);
			}catch (Exception e) {
				if (e.getCause() instanceof WebClientResponseException) {

					WebClientResponseException cause = (WebClientResponseException) e.getCause();
					System.out.println(cause.getResponseBodyAsString());
					System.out.println(cause.getMostSpecificCause());
					System.out.println(cause.getMessage());
					String storedMessage = "Error Message: " + cause.getMessage() + "Specific Cause: "
							+ cause.getMostSpecificCause() + "Response Body: " + cause.getResponseBodyAsString();
					StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementation.fileName);
					StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementation.fileName);
					return new AsyncResult<Void>(null);
					
				} else if (e.getCause() instanceof WebClientRequestException) {
					WebClientRequestException cause = (WebClientRequestException) e.getCause();
					System.out.println(cause.getRootCause());
					System.out.println(cause.getMostSpecificCause());
					System.out.println(cause.getMessage());
					
					String storedMessage = "Error Message: " + cause.getMessage() + "Specific Cause: "
							+ cause.getMostSpecificCause() + "Root Cause: " + cause.getRootCause();
					StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementation.fileName);
					StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementation.fileName);
					return new AsyncResult<Void>(null);

				} else {

					System.out.println(e.getLocalizedMessage());
					System.out.println(e.getMessage());
					System.out.println(e.getCause());
					String storedMessage = "Error Message: " + e.getMessage() + "Cause: " + e.getCause() + "Message: "
							+ e.getLocalizedMessage();
					StoreReservExceptionToBlob.updateToLatestFolder(storedMessage, "Reservations",TCAConfirmReservationServiceImplementation.fileName);
					StoreReservExceptionToBlob.storingExceptionInArchiveLocation(storedMessage,"Reservations", TCAConfirmReservationServiceImplementation.fileName);
					return new AsyncResult<Void>(null);
				}

			}
			

	}
}