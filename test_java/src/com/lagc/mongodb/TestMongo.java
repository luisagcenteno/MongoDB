package com.lagc.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.ServerAddress;

import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TestMongo {
	
	MongoClient mongoClient;
	DB db;
	
	public void setMongoClient(MongoClient mongoClient){
		
		this.mongoClient = mongoClient;
	}
	
	public void initialize(){
		
		db = mongoClient.getDB( "mydb" );
	}
	
	public void executeInsert(){
		
		DBCollection coll = db.getCollection("collection");
		
		coll.insert(constructObject("first"));
	}

	public DBObject getFirst(){
		
		DBCollection coll = db.getCollection("collection");
		
		return coll.findOne();
	}
	
	public void insertHundred(){
		
		DBCollection coll = db.getCollection("collection");
		
		for(int i=0; i < 100; i++){
			coll.insert(new BasicDBObject("i", i));
		}
		
	}
	
	public long getCount(){
		
		DBCollection coll = db.getCollection("collection");
		
		return coll.getCount();
	}
	
	public void printAll(){

		DBCollection coll = db.getCollection("collection");
		
		DBCursor cursor = coll.find();
		
		try{
			while(cursor.hasNext()){
				System.out.println(cursor.next());
			}
		}finally{
			cursor.close();
		}
	}
	
	public void queryOne(int i){
		
		DBCollection coll = db.getCollection("collection");
		
		BasicDBObject query = new BasicDBObject("i", i);
		
		DBCursor cursor = coll.find(query);
		
		try{
			while(cursor.hasNext()){
				System.out.println(cursor.next());
			}
				
		}finally{
			cursor.close();
		}
	}
	
	public void queryGreaterThan(int i){
		DBCollection coll = db.getCollection("collection");
		
		BasicDBObject query = new BasicDBObject("i", new BasicDBObject("$gt", i));
		
		DBCursor cursor = coll.find(query);
		
		try{
			while(cursor.hasNext()){
				System.out.println(cursor.next());
			}
			
		}finally{
			cursor.close();
		}
		
	}
	
	public void queryBetween(int i, int j){
		
		DBCollection coll = db.getCollection("collection");
		
		BasicDBObject query = new BasicDBObject("i", new BasicDBObject("$gt", i).append("$lte", j));
		
		DBCursor cursor = coll.find(query);
		
		try{
			while(cursor.hasNext()){
				System.out.println(cursor.next());
			}
		}finally{
			cursor.close();
		}
		
	}
	
	public BasicDBObject constructObject(String obj){
		
		BasicDBObject result;
		
		switch(obj){
			case "first":
				result = new BasicDBObject("name", "MongoDB")
					.append("type", "database")
					.append("count", 1)
					.append("info", new BasicDBObject("x", "203").append("y", 102));
			break;
			default:
				result = new BasicDBObject();
			
		}
		
		return result;
	}
	
	public static void main(String args[]){
		
		
		// To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
		// if it's a member of a replica set:
		//MongoClient mongoClient = new MongoClient();
		// or
		//MongoClient mongoClient = new MongoClient( "localhost" );
		// or
		//MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
		// or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
		//MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017),
		//                                      new ServerAddress("localhost", 27018),
		//                                      new ServerAddress("localhost", 27019)));

		
		TestMongo test = new TestMongo();

		test.setMongoClient(new MongoClient());
		
		test.initialize();

		//test.executeInsert();
		//System.out.println(test.getFirst());
		
		//test.insertHundred();
		
		//System.out.println(test.getCount());
		
		//test.printAll();
		
		//test.queryOne(Integer.parseInt(args[0]));
		
		//test.queryGreaterThan(Integer.parseInt(args[0]));
		
		test.queryBetween(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
	}
}
