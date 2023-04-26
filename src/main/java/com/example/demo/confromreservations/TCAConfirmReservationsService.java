package com.example.demo.confromreservations;

import java.util.concurrent.Future;

import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;

public interface TCAConfirmReservationsService {

	@Retryable(include =  { Exception.class }, maxAttempts = 3, backoff = @Backoff(delay = 100))
	public Future<Void> getConfirmReservations(String accessToken, String dataList) throws Exception;

	
	@Recover
	public void storeExceptionIntoBlob(Exception e);
}
