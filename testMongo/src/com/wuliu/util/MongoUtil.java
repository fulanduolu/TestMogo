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
 *	Mongo������
 */
public class MongoUtil {
	
	private static MongoClient mongo = null;
	
	@Test
	public void test1(){
		DBCollection collection = getConnection("people","student");
//		insertOne(collection, "����ɽ��ҹ","�޾�","��ԭ�ú�");
		List<DBObject> list=findByName(collection, "name","����");
		System.out.println(list.get(0).get("like"));
//		updateOne(collection, new String[]{"name","ܽ��"}, "like","������������`");
//		delete(collection, "name", "laowang");
//		Map<String,String> map = new HashMap<String,String>();
//		map.put("name", "����");
//		map.put("age","����" );
//		map.put("like", "���");
//		map.put("likes", "����");
//		map.put("�Ը�", "�ޱ���");
//		insertOne(collection, map);
	}
	/**
	 * ����mongodb������
	 * @return
	 */
	public static DBCollection getConnection(String db, String collection) {
		
		DBCollection Collection = null;
		try {
			// ����mongo����
			mongo = new MongoClient("localhost", 27017);
			//�����ݿ�
			DB dB = OpenDb(mongo, db);
			//���ĵ�
			Collection = dB.getCollection(collection);

			System.out.println("���ӵ����ݿ�ɹ�==");
		} catch (Exception e) {
			System.out.println("�������ݿ��쳣==");
			e.printStackTrace();
		}
		return Collection;
	}
	/**
	 * �ر�����
	 */
	public static void CloseConnection(MongoClient mongo){
		if(mongo!=null){
			//��ʾ���Ӳ�λ��
			mongo.close();
		}
	}
	/**
	 * ��ָ�����ݿ�,�����ظ����ݿ�
	 */
	public static DB OpenDb(MongoClient mongo , String dbName){
//		MongoDatabase mongoDatabase = mongo.getDatabase(dbName);
//		return mongoDatabase;
		DB db = new DB(mongo,dbName);
		return db;
	}
	/**
	 * ����ָ���ĵ�(orcle�нб�mongo�н��ĵ�)
	 */
	public static DBCollection ConnDocument(DB db,String doName){
		DBCollection collection = db.getCollection(doName);    //����ָ���ĵ�
		return collection;
	}
	/**
	 * ������Ӳ���
	 * 
	 */
	public void insertOne(DBCollection collection , Map<String,String> map){
		Set<Entry<String,String>> entry = map.entrySet();
		//�����ĵ�����
		DBObject content = new BasicDBObject();
		for(Entry e:entry){
			String key = e.getKey().toString();
			String value = e.getValue().toString();
			//�����Ƕ����뵽content��
			content.put(key, value);
		}
		collection.insert(content);
	}
	/**
	 * ��ѯ�ķ���
	 * @param collection �򿪵��ĵ�
	 * @param obj1    �����ֶ���
	 * @param obj2    �����Ӧ��ֵ
	 */
	public static List<DBObject> findByName(DBCollection collection,String obj1,String obj2){
		//������ѯ�ĵ�
		DBObject bson = new BasicDBObject();
		bson.put(obj1, obj2);
		List<DBObject> list=new ArrayList<DBObject>();
		//�õ�������ģ��
		DBCursor cursor = collection.find(bson);
		while(cursor.hasNext()){
			DBObject content=cursor.next();
			list.add(content);
		}
		return list;
	}
	/**
	 * ���������޸�
	 * @param collection  ���ӵ�ָ���ĵ�(��)
	 * @param str	      ԭ��ֵ�����飨0���ֶ��� 1��ֵ��
	 * @param obj1	      Ҫ�޸ĵ��ֶ�
	 * @param obj2           �޸ĵ�ֵ
	 */
	public static void updateOne(DBCollection collection,String[] str,String obj1,String obj2){
		//����֮ǰ��ֵ
		DBObject old_bson = new BasicDBObject();
		old_bson.put(str[0],str[1]);
		//�޸�֮���ֵ
		DBObject new_bson = new BasicDBObject();
		DBObject help = new BasicDBObject();
		new_bson.put(obj1, obj2);
		//������Ϣ���޸�
		help.put("${set}", new_bson);
		collection.update(old_bson, new_bson);
	}
	/**
	 * ɾ��Ԫ��
	 * @param obj1  �ֶ���
	 * @param obj2  �ֶ�ֵ
	 */
	public static void delete(DBCollection collection,String obj1,String obj2){
		DBObject content = new BasicDBObject();
		content.put(obj1, obj2);
		collection.remove(content);
	}
	
}
