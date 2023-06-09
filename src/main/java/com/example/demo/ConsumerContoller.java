package com.example.demo;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.adls.UploadReservationDataToADLS;
import com.example.demo.adls.avalon.UploadReservationDeltaDataToADLS;
import com.example.demo.confromreservations.TCAConfirmReservations;
import com.example.demo.consumer.ConsumerFromReservationDeltaEhub;
import com.example.demo.consumer.ConsumerFromReservationEhub;

@RestController
public class ConsumerContoller {
	
	
	@Autowired
	ConsumerFromReservationEhub cons;
	
	@Autowired
	UploadReservationDataToADLS upload;
	
	@Autowired
	TCAConfirmReservations confirmReservations;
	
	@Autowired
	ConsumerFromReservationDeltaEhub consumerReservDelta;
	
	@Autowired
	UploadReservationDeltaDataToADLS uploadAvalon;
	
	@GetMapping("/consumer")
	public String consumerEventData(@RequestParam(name="totalEvent") long totalEvents, @RequestParam(name="loginToken") String loginToken) {
		
		
		try {
			String consumerFromEventHub = cons.consumerFromEventHub(totalEvents );
			System.out.println("EventHubConsumption: "+consumerFromEventHub);
			String uploadToADLS = upload.uploadToADLS(ConsumerFromReservationEhub.dataList);
			System.out.println("Upload to adls: "+uploadToADLS);
			String callConfirmReservationsAPI = confirmReservations.callConfirmReservationsAPI(loginToken, ConsumerFromReservationEhub.dataList);
			System.out.println("ConfirmReservervations"+callConfirmReservationsAPI);
			return consumerFromEventHub+uploadToADLS+callConfirmReservationsAPI;
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		
	}
	
	@GetMapping("/avalon/resevDelta/consumer")
	public String consumerReservationDeltaEventData(@RequestParam(name="totalEvent") long totalEvents, @RequestParam(name="loginToken") String loginToken) {
		
		
		try {
			String consumerFromEventHub = consumerReservDelta.consumerFromEventHub(totalEvents );
			System.out.println("EventHubConsumption: "+consumerFromEventHub);
			String uploadToADLS = uploadAvalon.uploadToADLS(ConsumerFromReservationDeltaEhub.dataList);
			System.out.println("Upload to adls: "+uploadToADLS);
			
			return consumerFromEventHub+uploadToADLS;
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
		
	}

}
