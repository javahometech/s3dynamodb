package com.amazonaws.lambda.s3dynamodb;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class LoadS3DataToDynamoDB implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private String DYNAMODB_TABLE_NAME = "Employee";
	private String S3_BUCKET_NAME = "kammana";
	private String S3_FILE_NAME = "demo.csv";
	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	private DynamoDB dynamoDb = new DynamoDB(client);

	@Override
	public String handleRequest(S3Event event, Context context) {
		String bucket = S3_BUCKET_NAME;
		String key = S3_FILE_NAME;
		try {
			S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
			S3ObjectInputStream is = response.getObjectContent();
			Table table = dynamoDb.getTable(DYNAMODB_TABLE_NAME);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = br.readLine()) != null) {
				String[] data = line.split(",");
				Item item = new Item()
						.withPrimaryKey("Issue_ID", data[0])
						.withString("Offering_ID", data[1])
						.withString("Base CUSIP", data[2])
						.withString("Expected Closing Date", data[3])
						.withString("Issue Offering Amount", data[4])
						.withString("Issuer_ID", data[5]);
				table.putItem(item);
			}
			String contentType = response.getObjectMetadata().getContentType();
			context.getLogger().log("CONTENT TYPE: " + contentType);
			return "Data successfully lodade to "+DYNAMODB_TABLE_NAME+" table";
		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format("Error getting object %s from bucket %s. Make sure they exist and"
					+ " your bucket is in the same region as this function.", key, bucket));
			return "Error "+e.getMessage();
		}
	}
}