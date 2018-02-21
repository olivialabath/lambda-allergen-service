package olivialabath.lambda_allergen_service.java;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

// POJO used to write items into DynamoDB
@DynamoDBTable(tableName = "Allergens")
public class Allergen {
	long date;
	String name;
	int count;
	
	@DynamoDBHashKey
	public long getDate() {
		return date;
	}
	public void setDate(long date) {
		this.date = date;
	}
	
	@DynamoDBRangeKey
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	@DynamoDBAttribute
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	public Allergen(long date, String name, int count) {
		this.date = date;
		this.name = name;
		this.count = count;
	}
	
	public Allergen(){
		
	}
	
	public String toString() {
		return "(" + date + " : " + name + " : " + count + ")";
	}
}
