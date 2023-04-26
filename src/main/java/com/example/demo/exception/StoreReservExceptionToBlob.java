package com.example.demo.exception;

import java.time.ZonedDateTime;
import java.util.UUID;

import com.azure.core.util.BinaryData;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;

public class StoreReservExceptionToBlob {
	private static final String DIRECTORYNAME="RAW";
	private static final String SUBDIRECTORYNAME="TCA-API";
	private static final String SUBDIRECTORYNAME1="PipelineErrors";
	private static final String SUBDIRECTORYNAME2="Latest";
	private static final String ARCHIVE="Archive";

	public static void storingExceptionInArchiveLocation(String exceptionMessage, String moduleName, String folderName) {
		DataLakeServiceClient getDataLakeServiceClient = GetDataLakeServiceClient("wohalgpmsdldevsa", "iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==");

		DataLakeFileSystemClient fileSystemClient = getDataLakeServiceClient
				.getFileSystemClient("woh-alg-pms-datalake-dev");

		
	ZonedDateTime currentInstant = ZonedDateTime.now();
	int year = currentInstant.getYear();
	int month = currentInstant.getMonthValue();
	int day = currentInstant.getDayOfMonth();
	int hour = currentInstant.getHour();
	int minute = currentInstant.getMinute();
	int second = currentInstant.getSecond();
	String fileName = folderName+"_"+moduleName+"_"+year+"-"+month+"-"+day+"_"+hour+"-"+minute+"-"+second+".json";
	
		DataLakeDirectoryClient subdirectoryClient = fileSystemClient.createDirectoryIfNotExists(DIRECTORYNAME)
				                                    .createSubdirectoryIfNotExists(SUBDIRECTORYNAME)
				                                    .createSubdirectoryIfNotExists(SUBDIRECTORYNAME1)
				                                    .createSubdirectoryIfNotExists(ARCHIVE)
				                                    .createSubdirectoryIfNotExists(ARCHIVE+"_"+year+"-"+month+"-"+day)
				                                    .createSubdirectoryIfNotExists(ARCHIVE+"_"+year+"-"+month+"-"+day+"_"+hour+"-"+minute+"-"+second+UUID.randomUUID().toString())
				                                    .createSubdirectoryIfNotExists(moduleName)
				                                    .createSubdirectoryIfNotExists(folderName);
		DataLakeFileClient fileClient = subdirectoryClient.createFileIfNotExists(fileName);
		String msg = exceptionMessage.trim();
		fileClient.upload(BinaryData.fromString(msg), true);
		System.out.println("Error stored in path: "+fileClient.getFilePath()+" and filename is: "+fileName);
	}
	
	private static DataLakeServiceClient GetDataLakeServiceClient (String accountName, String accountKey){

	    StorageSharedKeyCredential sharedKeyCredential =
	        new StorageSharedKeyCredential(accountName, accountKey);

	    DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();

	    builder.credential(sharedKeyCredential);
	    builder.endpoint("https://" + accountName + ".dfs.core.windows.net");

	    return builder.buildClient();
	}
	

	public static void updateToLatestFolder(String exceptionMessage, String moduleName, String fileName) {
		DataLakeServiceClient getDataLakeServiceClient = GetDataLakeServiceClient("wohalgpmsdldevsa", "iSh4zGYXanmJdEhCSv/Qg1h+GF37rsfZAwIzzo0nByAgg6itXlDVQHFVe2gf5vK+3l4eFvtPWaVNj2P4f0wQow==");

		DataLakeFileSystemClient fileSystemClient = getDataLakeServiceClient
				.getFileSystemClient("woh-alg-pms-datalake-dev");

		
		DataLakeDirectoryClient subdirectoryClient = fileSystemClient.createDirectoryIfNotExists(DIRECTORYNAME)
                .createSubdirectoryIfNotExists(SUBDIRECTORYNAME)
                .createSubdirectoryIfNotExists(SUBDIRECTORYNAME1)
                .createSubdirectoryIfNotExists(SUBDIRECTORYNAME2)
                .createSubdirectoryIfNotExists(UUID.randomUUID().toString())
                .createSubdirectoryIfNotExists(moduleName);
		fileName = fileName+"_"+moduleName+".json";
		DataLakeFileClient fileClient = subdirectoryClient.createFile(fileName,true);
		String msg = exceptionMessage.trim();
		fileClient.upload(BinaryData.fromString(msg), true);
		System.out.println("Thread: "+Thread.currentThread().getName()+"Error message uploaded successfully in path : "+fileClient.getFilePath()+ " Filename: "+fileName);
	}

	
}
