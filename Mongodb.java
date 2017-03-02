import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;


public class Mongodb {
	public static void main(String[] args) throws MongoException, IOException{
		mongodb();
	}
	
	public static void mongodb() throws MongoException, IOException{

        //链接数据库
        Mongo connection = new Mongo("192.168.235.20",27017); //url与端口
        DB db = connection.getDB("weiboUser");//数据库名字
        boolean ok = db.authenticate("root", "iiip".toCharArray());  //认证过程，用户名（String）和密码（char[]）

        if(ok){
            System.out.println("db connection success!");

            //插入数据，mongodb的默认_id为主键，
            //在默认的情况下会自己添加哈希值，建议自定义_id，不然很可能会插入很多重复
            DBCollection collection = db.getCollection("Region"); 
            //最基本的属性
            BasicDBObject attribute = new BasicDBObject();
            attribute.put("_id", "5");
            attribute.put("电影名","星球大战");
           
           //地区（太多进行读取）
           BasicDBObject region = new BasicDBObject();
           String path = "/home/hadoop/Seeing项目内容/result/属性/星球大战外传：侠盗一号(100120178675)/地区.txt";
           String region_line;
           BufferedReader br = new BufferedReader(new FileReader(new File (path)));
           while((region_line = br.readLine()) != null){
        	   String[] result = region_line.split(",");
        	   region.put(result[0], result[1]);	   
           }
           attribute.put("地区", region);
            collection.insert(attribute);
        }
        else{
            System.out.println("connection error");
        }
    }
}
