package com.example.demo.confromreservations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class TCAConfirmReservations {
	
	@Autowired
	TCAConfirmReservationServiceImplementation confirmReservationService;
	
	public String callConfirmReservationsAPI(String accessToken, Set<String> dataList) throws Exception{
		Collection<Future<Void>> futures = new ArrayList<Future<Void>>();
		System.out.println("ConfirmReservations api call start");
		long before = System.currentTimeMillis();
		for (String data : dataList) {
			futures.add(confirmReservationService.getConfirmReservations(accessToken, data));
			
		}
		for (Future<Void> future : futures) {
			future.get();
		}

		long after = System.currentTimeMillis();

		String reservationTime = "Time it took for all reservations data to make confirm reservations call to TCA API: " + (after - before) / 1000.0 + " seconds.\n";
	System.out.println(reservationTime);
	return reservationTime;
	}

}
