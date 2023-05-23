package com.example.demo.adls.avalon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;



@Component
public class UploadReservationDeltaDataToADLS {

	@Autowired
	UploadReservDeltaDataToADLSService conf;

	public String uploadToADLS(Set<String> eventDataList) throws Exception{
		Collection<Future<Void>> futures = new ArrayList<Future<Void>>();
		long before = System.currentTimeMillis();
		for (String data : eventDataList) {
			futures.add(conf.uploadReservDeltaDataToADLSStorage(data));
		}
		for (Future<Void> future : futures) {
			future.get();
		}

		long after = System.currentTimeMillis();
String timeTakenToUploadReservationDataToADLS ="Time it took for all reservations delta data to be stored in ADLS: " + (after - before) / 1000.0 + " seconds.\n";
	return timeTakenToUploadReservationDataToADLS;
	}

}
