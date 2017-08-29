package com.wuliu.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 *	Mongo的连接
 */
public class MongoUtil {
	
	private static MongoClient mongo = null;
	
	@Test
	public void test1(){
		DBCollection collection = getConnection("people","student");
//		insertOne(collection, "蓬莱山辉夜","无尽","藤原妹红");
		List<DBObject> list=findByName(collection, "name","琴心");
		System.out.println(list.get(0).get("like"));
//		updateOne(collection, new String[]{"name","芙兰"}, "like","让美玲陪着玩`");
//		delete(collection, "name", "laowang");
//		Map<String,String> map = new HashMap<String,String>();
//		map.put("name", "琴心");
//		map.put("age","？？" );
//		map.put("like", "面具");
//		map.put("likes", "恋恋");
//		map.put("性格", "无表情");
//		insertOne(collection, map);
	}
	/**
	 * 建立mongodb的连接
	 * @return
	 */
	public static DBCollection getConnection(String db, String collection) {
		
		DBCollection Collection = null;
		try {
			// 开启mongo服务
			mongo = new MongoClient("localhost", 27017);
			//打开数据库
			DB dB = OpenDb(mongo, db);
			//打开文档
			Collection = dB.getCollection(collection);

			System.out.println("连接到数据库成功==");
		} catch (Exception e) {
			System.out.println("连接数据库异常==");
			e.printStackTrace();
		}
		return Collection;
	}
	/**
	 * 关闭连接
	 */
	public static void CloseConnection(MongoClient mongo){
		if(mongo!=null){
			//表示连接部位空
			mongo.close();
		}
	}
	/**
	 * 打开指定数据库,并返回该数据库
	 */
	public static DB OpenDb(MongoClient mongo , String dbName){
//		MongoDatabase mongoDatabase = mongo.getDatabase(dbName);
//		return mongoDatabase;
		DB db = new DB(mongo,dbName);
		return db;
	}
	/**
	 * 连接指定文档(orcle中叫表，mongo中叫文档)
	 */
	public static DBCollection ConnDocument(DB db,String doName){
		DBCollection collection = db.getCollection(doName);    //连接指定文档
		return collection;
	}
	/**
	 * 进行添加操作
	 * 
	 */
	public void insertOne(DBCollection collection , Map<String,String> map){
		Set<Entry<String,String>> entry = map.entrySet();
		//声明文档对象
		DBObject content = new BasicDBObject();
		for(Entry e:entry){
			String key = e.getKey().toString();
			String value = e.getValue().toString();
			//将他们都加入到content中
			content.put(key, value);
		}
		collection.insert(content);
	}
	/**
	 * 查询的方法
	 * @param collection 打开的文档
	 * @param obj1    输入字段名
	 * @param obj2    输入对应的值
	 */
	public static List<DBObject> findByName(DBCollection collection,String obj1,String obj2){
		//简历查询文档
		DBObject bson = new BasicDBObject();
		bson.put(obj1, obj2);
		List<DBObject> list=new ArrayList<DBObject>();
		//得到迭代器模型
		DBCursor cursor = collection.find(bson);
		while(cursor.hasNext()){
			DBObject content=cursor.next();
			list.add(content);
		}
		return list;
	}
	/**
	 * 进行数据修改
	 * @param collection  连接的指定文档(表)
	 * @param str	      原来值得数组（0：字段名 1：值）
	 * @param obj1	      要修改的字段
	 * @param obj2           修改的值
	 */
	public static void updateOne(DBCollection collection,String[] str,String obj1,String obj2){
		//更改之前的值
		DBObject old_bson = new BasicDBObject();
		old_bson.put(str[0],str[1]);
		//修改之后的值
		DBObject new_bson = new BasicDBObject();
		DBObject help = new BasicDBObject();
		new_bson.put(obj1, obj2);
		//进行信息的修改
		help.put("${set}", new_bson);
		collection.update(old_bson, new_bson);
	}
	/**
	 * 删除元素
	 * @param obj1  字段名
	 * @param obj2  字段值
	 */
	public static void delete(DBCollection collection,String obj1,String obj2){
		DBObject content = new BasicDBObject();
		content.put(obj1, obj2);
		collection.remove(content);
	}
	
}
