package com.example.demo.adls;

import java.time.ZonedDateTime;
import java.util.concurrent.Future;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeRequestConditions;
import com.azure.storage.file.datalake.models.DownloadRetryOptions;
import com.azure.storage.file.datalake.models.FileRange;
import com.azure.storage.file.datalake.models.PathHttpHeaders;
import com.azure.storage.file.datalake.models.PathInfo;

@Component
public class UploadReservDataToADLSService {
	
	@Async("consumerAsyncExecutor")
	public Future<Void> uploadReservDataToADLSStorage(String data) {
		
		System.out.println("Invoking an asynchronous method. " 
				+ Thread.currentThread().getName());
		DataLakeServiceClient getDataLakeServiceClient = GetDataLakeServiceClient("wohalgpmsdldevsa", "iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==");

		DataLakeFileSystemClient fileSystemClient = getDataLakeServiceClient
				.getFileSystemClient("woh-alg-pms-datalake-dev");

		ZonedDateTime currentTime = ZonedDateTime.now();
		DataLakeDirectoryClient directoryClient = fileSystemClient.createDirectoryIfNotExists("ehub-poc-reserv-data")
				.createSubdirectoryIfNotExists(String.valueOf(currentTime.getYear()))
				.createSubdirectoryIfNotExists(String.valueOf(currentTime.getMonthValue()))
				.createSubdirectoryIfNotExists(String.valueOf(currentTime.getDayOfMonth()));


		String timeStamp= String.valueOf(currentTime.getYear())+"-"
                +String.valueOf(currentTime.getMonthValue())+"-"
		         +String.valueOf(currentTime.getDayOfMonth())+"_"
		         +String.valueOf(currentTime.getHour())+"-"
		         +String.valueOf(currentTime.getMinute())+"-"
		         +String.valueOf(currentTime.getSecond());
		
		try {
			
			JSONObject jsonObject = (JSONObject) new JSONParser().parse(data);
			String reservationData = (String) jsonObject.get("ReservationData");
			String fileName = (String) jsonObject.get("group_code") + "_" + (String) jsonObject.get("brand_code") + "_"
					+ (String) jsonObject.get("hotel_code") + "_Reservations_" + timeStamp + ".json";
			DataLakeFileClient fileClient = directoryClient.createFile(fileName);
			String trimmedString = reservationData.trim();
			//fileClient.append(BinaryData.fromString(trimmedString), 0);
			fileClient.upload(BinaryData.fromString(trimmedString), true);
			System.out.printf(Thread.currentThread().getName()+"Flush of reservation data completed on path: %s%n", fileClient.getFilePath());
			
			//boolean writeToDataLakeBlobStorageWithResponse = writeToDataLakeBlobStorageWithResponse(trimmedString.length(),fileClient);
			
			 if(reservationData != null && !reservationData.isBlank()) {
				 
				 uploadToConfirmedReservations(trimmedString,fileName, currentTime);
			 }
		}catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new AsyncResult<Void>(null);
	}
	
	static public DataLakeServiceClient GetDataLakeServiceClient(String accountName, String accountKey){

	    StorageSharedKeyCredential sharedKeyCredential =
	        new StorageSharedKeyCredential(accountName, accountKey);

	    DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

	    builder.credential(sharedKeyCredential);
	    builder.endpoint("https://" + accountName + ".dfs.core.windows.net");

	    return builder.buildClient();
	}
	
	private static boolean writeToDataLakeBlobStorageWithResponse(int i, DataLakeFileClient fileClient) {
		FileRange range = new FileRange(1024, 2048L);
		 DownloadRetryOptions options = new DownloadRetryOptions().setMaxRetryRequests(5);
		 byte[] contentMd5 = new byte[0]; // Replace with valid md5
		 boolean retainUncommittedData = false;
		 boolean close = false;
		 PathHttpHeaders httpHeaders = new PathHttpHeaders()
		     .setContentLanguage("en-US")
		     .setContentType("binary");
		 DataLakeRequestConditions requestConditions = new DataLakeRequestConditions();


		 Response<PathInfo> response = fileClient.flushWithResponse(i, retainUncommittedData, close, httpHeaders,
			     requestConditions, null, Context.NONE);
		 if(response.getStatusCode() == 200) {
			 System.out.printf(Thread.currentThread().getName()+"Flush data completed on path: %s with status", fileClient.getFilePath(), response.getStatusCode());
			 return true;
		 } else {
			 
			 return false;
		 }
	}
	
	private static void uploadToConfirmedReservations(String trimmedString,String fileName, ZonedDateTime currentTime) {
		DataLakeServiceClient getDataLakeServiceClient = GetDataLakeServiceClient("wohalgpmsdldevsa", "iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==");

		DataLakeFileSystemClient fileSystemClient = getDataLakeServiceClient
				.getFileSystemClient("woh-alg-pms-datalake-dev");
		
		//
		DataLakeDirectoryClient subdirectoryClient = fileSystemClient.getDirectoryClient("RAW").getSubdirectoryClient("TCA-API").getSubdirectoryClient("Reservations").getSubdirectoryClient("ConfirmedReservations");
		DataLakeFileClient fileClient1 = subdirectoryClient.createFile(fileName);
		//fileClient1.append(BinaryData.fromString(trimmedString), 0);
		fileClient1.upload(BinaryData.fromString(trimmedString), true);
		//writeToDataLakeBlobStorageWithResponse(trimmedString.length(),fileClient1);
		
		System.out.printf(Thread.currentThread().getName()+"Flush of reservation data completed on path: %s%n ", fileClient1.getFilePath());
	}
}
