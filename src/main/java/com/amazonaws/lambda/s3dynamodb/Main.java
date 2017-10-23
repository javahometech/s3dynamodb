package com.amazonaws.lambda.s3dynamodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Main {
	private static String DYNAMODB_TABLE_NAME = "Employee";
	public static void main(String[] args) throws IOException {
		AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
		AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDb = new DynamoDB(dynamoDBClient);
		Table table = dynamoDb.getTable(DYNAMODB_TABLE_NAME);
		
		S3Object response = s3.getObject(new GetObjectRequest("kammana", "s3-demo.csv"));
		S3ObjectInputStream is = response.getObjectContent();
		
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
	}
}
