package classes;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import sbd.model.Triple;
import scala.Tuple2;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;

public class SchemaExtractor implements Serializable {

	public ArrayList<String> fromCSV(String path, String del) throws IOException {

		CSVReader reader = new CSVReader(new FileReader(path));
		String[] header = reader.readNext();

		ArrayList<String> headerArrayList = new ArrayList<String>(Arrays.asList(header));
		return headerArrayList;
	}

	@SuppressWarnings("unchecked")
	public void fromSemData(String path, String del, String dsName, String dsIRI) throws IOException, ClassNotFoundException {

		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("spark://139.18.2.34:3077");
		//SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("spark://139.18.2.34:3077").set("spark.executor.memory", "60g").set("spark.rdd.compress","true").set("spark.storage.memoryFraction","1").set("spark.core.connection.ack.wait.timeout","600").set("spark.akka.frameSize","50");
		//		.setJars(new String[]{"/mnt/SparkServlet.jar"});
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf); // ctx.addJar("/mnt/SparkServlet.jar");
		
		try {
		
			SQLContext sqlContext = new SQLContext(ctx.sc());
	
			// 1. Read text file
			JavaRDD<String> lines = ctx.textFile(path);
	
			// 2. Map lines to Triple objects
			@SuppressWarnings("resource")
			JavaRDD<Triple> triples = lines.map(new Function<String,Triple>() {
	
				@Override
				public Triple call(String line) throws Exception {
					
					String[] parts = line.split(del);
	
					Triple triple = null;
					
					if (parts[1].equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
						triple = new Triple(replaceInValue(removeTagSymbol(parts[0])), null, replaceInValue(removeTagSymbol(parts[2])));
					else {
						String subject = replaceInValue(removeTagSymbol(parts[0])); /** MEASURE removeTagSymbol() time **/
						String property = replaceInColumn(removeTagSymbol(parts[1]));
						String object = reverse(replaceInValue(removeTagSymbol(parts[2])));
						
						triple = new Triple(subject, property, object); 
					}
					
					return triple;
				}	
			});
	
			// 3. Map Triple objects to pairs (Triple.subject,[Triple.property, Triple.object])
			@SuppressWarnings({ "rawtypes" })
			JavaPairRDD<String, Tuple2<String, String>> subject_property = triples.mapToPair(new PairFunction<Triple, String, Tuple2<String, String>>() {
	
				@Override
				public Tuple2<String, Tuple2<String, String>> call(Triple trpl) throws Exception {
					return new Tuple2(trpl.getSubject(), new Tuple2(trpl.getProperty(), trpl.getObject()));
				}
			});
			
			// 4. Group pairs by subject => s,(p,o)[]
			JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupBySubject = subject_property.groupByKey();
	
			// 5. Map to pairs (Type,(s,(p,o)[]))
			@SuppressWarnings({ "serial" })
			JavaPairRDD<String, Tuple2<String, Iterable<Tuple2<String, String>>>> type_s_po = groupBySubject.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, Tuple2<String, Iterable<Tuple2<String, String>>>>() {
				@Override
				public Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>> call(Tuple2<String, Iterable<Tuple2<String, String>>> list)
						throws Exception {
					List<Tuple2<String, String>> p_o = new ArrayList<Tuple2<String, String>>();
					List<String> types = new ArrayList<String>();
					String property = null, lastType = null;
					String object = null;
					Tuple2<String, String> tt = null;
					Tuple2<String, String> t2 = null;
	
					String subject = list._1();
					Iterator<Tuple2<String, String>> it = list._2().iterator();
					while (it.hasNext()) {
						tt = it.next();
						property = tt._1();
						object = tt._2();
						if (property == null) {
							lastType = (String) object;
							p_o.add(new Tuple2<String, String>("type_" + object, "1"));
							types.add(object);
						} else {
							// Form Tuple2(P,O)
							t2 = new Tuple2<String, String>(property, object);
							p_o.add(t2);
						}
					}
					
					Collections.sort(types); // order types lexicographically then select the last one => similar instances end up in same table
					
					//String chosen_type = lastType; // The last type is generally the most specific, but this is definitely not a rule.
					String chosen_type = types.get(types.size()-1);
					
					// We might use a hierarchy of classes from the schema if provided in future
					p_o.remove(new Tuple2<String, Object>("type_" + chosen_type, "1"));
					
					Tuple2 s_po = new Tuple2(subject, p_o);
					Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>> result = new Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>>(chosen_type, s_po);
					return result;
				}
			});
	
			// 6. Group by type => (type, It(s, It(p, o)))
			JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> groupByType = type_s_po.groupByKey();		
			
			// 7. Get all the types
			//groupByType: <String, Iterable<Tuple2<String, Iterable<Tuple2<String, Object>>>>>
			List<String> keys = groupByType.keys().distinct().collect();
	
			// 8. Iterate through all types
			//int t = 0;
			for (String key : keys) {
				//t++;
				//if (t < 20) { // To remove later
				//if(key.contains("HistoricTower")){
				
					// 8.1 Get RDD of the type
					@SuppressWarnings("unused")
					JavaRDD<Tuple2<String, Iterable<Tuple2<String, String>>>> rddByKey = getRddByKey(groupByType, key);
	
		            // 8.2 Map the type RDD => Return type columns
		            //JavaRDD<LinkedHashMap<String, Object>> cols = rddByKey.map(i -> {
					JavaRDD<String> cols = rddByKey.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Tuple2<String,String>>>,String>(){
	
						@Override
						public Iterable<String> call(Tuple2<String, Iterable<Tuple2<String, String>>> i) throws Exception {
							LinkedHashMap<String, Object> po = new LinkedHashMap<String, Object>(); // a hashamp (that keeps order) to store all type's columns
			            	
			    			// 8.2.1 Iterate through all (p,o) and collect the columns (update incrementally the hashmap)
			    			Iterator<Tuple2<String, String>> iter = i._2.iterator(); // Through all subjects
	
							while (iter.hasNext()) {
								Tuple2<String, String> temp = iter.next(); // To avoid using .next() two times which advances in the iteration
								
								String property = temp._1();
								String object = reverse(temp._2());
	
								if(object.contains("XMLSchema#double")) {				
									if(!po.containsKey(property + "--TD") && !po.containsKey(property + "--TAD"))
										property = property + "--TD";
									else if(!po.containsKey(property + "--TAD")) {
										po.remove(property + "--TD");
										property = property + "--TAD";
									}
											
								} else if(object.contains("XMLSchema#int")) {
									property = property + "--TI";
									
									if(po.containsKey(property))
										property = property.replace("--TI", "--TAI");
									
								} else if(object.contains("XMLSchema#boolean")) {
									property = property + "--TB";							
								}
								
								po.put(property, "");// CAUTION: overwriting previous columns
			    			}
							// 8.2.2 At last, add the id column 
							po.put("id", "");
	
							//return po;
							return po.keySet();
						}
		    		});
		            
		            // 8.- Vars				
		            LinkedHashMap<String, Object> type_columns = new LinkedHashMap<String, Object>(); // a hashamp (that keeps order) to store all type's columns
		            String col = null;
		            
		    		// 8.3 Read columns and construct a hashmap
		    		List<String> readColumns = cols.distinct().collect();
		    		
		    		for (String j : readColumns) type_columns.put(j,""); // Overwrite original columns (collect() may return columns in different order than collected firstly)
		    		
		    		//System.out.println("HAAAA" + readColumns);
		    		
					// 8.4 Generate the Parquet table schema from the collected columns
					List<StructField> table_columns = new ArrayList<StructField>();
					HashMap<String,String> toSaveToDB = new HashMap<String,String>();
					
					
					for (String s : readColumns) {				
						/** TO MEASURE **/
						if(s.contains("--TD")) {
							if(!readColumns.contains(s.split("--")[0] + "--TAD")) {
								col = s.split("--")[0];
								table_columns.add(DataTypes.createStructField(col, DataTypes.DoubleType, true));
								toSaveToDB.put(col, "double");
							} else
								continue;						
						} else if(s.contains("--TI")) {
							col = s.split("--")[0];
							table_columns.add(DataTypes.createStructField(col, DataTypes.IntegerType, true));
							toSaveToDB.put(col, "int");
						} else if(s.contains("--TB")) {
							col = s.split("--")[0];
							table_columns.add(DataTypes.createStructField(col, DataTypes.BooleanType, true));
							toSaveToDB.put(col, "boolean");
						} else if(s.contains("--TAD")) {
							col = s.split("--")[0];
							table_columns.add(DataTypes.createStructField(col, DataTypes.createArrayType(DataTypes.DoubleType, true), true));
							toSaveToDB.put(col, "arrayDouble");
						} else if(s.contains("--TAI")) {
							col = s.split("--")[0];
							table_columns.add(DataTypes.createStructField(col, DataTypes.createArrayType(DataTypes.IntegerType, true), true));
							toSaveToDB.put(col, "arrayInt");
						} else {
							table_columns.add(DataTypes.createStructField(s, DataTypes.StringType, true));
							toSaveToDB.put(s, "string");
						} /** END TO MEASURE **/
						
					}
					//System.out.println("HOOOOO" + table_columns);
					
					// 8.5 Save columns to database
					saveToMongoDB(replaceInType(key), toSaveToDB, dsName, dsIRI);				
					
					StructType schema = DataTypes.createStructType(table_columns);
	
					// 8.6. Map RDD of (subject, Iter(property, object)) to an RDD of Row
		    		JavaRDD<Row> returnValues = rddByKey.map(new Function<Tuple2<String,Iterable<Tuple2<String,String>>>,Row>() {
		    			
						@Override
						public Row call(Tuple2<String, Iterable<Tuple2<String, String>>> i) throws Exception {
	
			    			Row values_list = null;
			    			LinkedHashMap<String, Object> po = new LinkedHashMap<String, Object>();
			    			
			    			// 8.6.1 Initialize the hashmap values with null (they're previously initialized with a String "", so if a certain value is an int => a cast error)
							for (String j : readColumns) { // TO INHENCE						
								if(j.contains("--TI")) 
									po.put(j.replace("--TI", ""),null);
								else if(j.contains("--TD") && !readColumns.contains(j + "--TAD"))
									po.put(j.replace("--TD", ""),null);
								else if(j.contains("--TB")) 
										po.put(j.replace("--TB", ""),null);
								else if(j.contains("--TAI")) 
									po.put(j.replace("--TAI", ""),null);
								else if(j.contains("--TAD")) 
									po.put(j.replace("--TAD", ""),null);
								else
									po.put(j,null);
							}
			    				    			
			    			// 8.6.2 Iterate through all the (property, object) pairs to save data in the collected columns
							String subject = i._1;					
			    			Iterator<Tuple2<String, String>> iter = i._2.iterator();
		
			    			int z = 0;
							while (iter.hasNext()) {						
								Tuple2<String, String> temp = iter.next(); // To avoid using .next() two times, which advances in the iteration
								
								String property = temp._1();
								String object = reverse(temp._2());
								Object newobject = null;
								
								if(readColumns.contains(property + "--TD") && !readColumns.contains(property + "--TAD")) {							
									newobject = Double.parseDouble(object.replace("^^www.w3.org/2001/XMLSchema#double","").replace("\"",""));
									po.put(property, newobject);
								} else if(readColumns.contains(property + "--TI")) {
									newobject = Integer.parseInt(object.replace("^^www.w3.org/2001/XMLSchema#integer","").replace("^^www.w3.org/2001/XMLSchema#int","").replace("\"",""));
									po.put(property, newobject);
								}  else if(readColumns.contains(property + "--TB")) {
									newobject = Boolean.parseBoolean(object.replace("^^www.w3.org/2001/XMLSchema#boolean","").replace("\"",""));
									po.put(property, newobject);
								} else if(readColumns.contains(property + "--TAD")) {
									ArrayList<Double> arr = null;
									newobject = Double.parseDouble(object.replace("^^www.w3.org/2001/XMLSchema#double","").replace("\"",""));
									if(po.get(property) != null){
										//System.out.println("TYPE (" + po.get(property) + "): ");
										arr = (ArrayList<Double>) po.get(property);
										arr.add((Double) newobject);									
									} else {
										//System.out.println("TYPE (" + po.get(property) + ")");
										arr = new ArrayList<Double>();
										arr.add((Double) newobject);
									}
									po.put(property, arr);
								} else if(readColumns.contains(property + "--TAI")) {
									ArrayList<Integer> arr = new ArrayList<Integer>();
									if(po.containsKey(property)){
										arr = (ArrayList<Integer>) po.get(property);
										arr.add((Integer) newobject);									
									} else {
										arr.add((Integer) newobject);
									}
									po.put(property, arr);
								} else
									po.put(property, object);
		
							}
							
			    			// 8.6.3 Add the subject finally as the ID to the hashmap	    			
			    			po.put("id", subject);
							
			    			//System.out.println("Values to be inserted under this schema: " + po.keySet());
							
							// 8.6.4 Create the row from the hashmap values
							List<Object> vals = new ArrayList<Object>(po.values());
							values_list = RowFactory.create(vals.toArray());
		
							return values_list;					
		    			}
		    		});
		    		
		    		// 8.7 Create an RDD by applying a schema to the RDD
					DataFrame typeDataFrame = sqlContext.createDataFrame(returnValues, schema);
					
					// 8.8 Save to Parquet table
					typeDataFrame.write().parquet("hdfs://akswnc5.informatik.uni-leipzig.de:54310/output/Parquets/" + replaceInType(key)); // or "/home/hadoop/tables/"
					//typeDataFrame.write().parquet("/mnt/output/Parquets/" + replaceInType(key)); // or "/home/hadoop/tables/"
					//typeDataFrame.write().parquet("hdfs://akswnc4.informatik.uni-leipzig.de:54310/output/Parquets/" + replaceInType(key));
					
				}
	        //}
			
			ctx.close();
			
		} catch (Exception ex) {
			System.out.println("SOMETHING WENT WRONG..." + ex.getMessage());
			ctx.close();
		}
	}

	private String reverse(String string) {
		return new StringBuffer(string).reverse().toString();
	}

	private JavaRDD getRddByKey(JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> pairRdd, String key) {
		
		JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> a = pairRdd.filter(new Function<Tuple2<String,Iterable<Tuple2<String,Iterable<Tuple2<String,String>>>>>,Boolean>(){
			
			@Override
			public Boolean call(Tuple2<String,Iterable<Tuple2<String,Iterable<Tuple2<String,String>>>>> v) throws Exception {
				// TODO Auto-generated method stub
				return v._1().equals(key);
			};
		});

		return a.values().flatMap(tuples -> tuples);
	}

	// Helping methods
	private String removeTagSymbol(String string) {
		String str = string.replace("<", "").replace(">", "");
		return str;
	}
	
	private String replaceInValue(String str) {
		String newStr = str.replace("http://", "");
		return newStr;
	}
	
	private String replaceInType(String str) {
		String newStr = str.replace("/", "__").replace("-", "@");
		return newStr;
	}	

	private String replaceInColumn(String str) {
		String newStr = str.replace("http://", "");
		return newStr;
	}
	
	private void saveToMongoDB(String fileName, HashMap<String,String> readColumns, String datasetName, String datasetIRI) {
		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("sebida");
		MongoCollection tables = database.getCollection("tables");
		MongoCollection<Document> datasets = database.getCollection("datasets");
		
		String tableName = fileName.replace(".","$");
		
		List<Document> documents = new ArrayList<Document>();
		for (Map.Entry<String,String> c : readColumns.entrySet()) {
		    documents.add(new Document("table", tableName).append("column", c.getKey()).append("type", c.getValue()));
		}
		tables.insertMany(documents);
		
		Document dataset = new Document();
		dataset.append("table", tableName).append("dataset", datasetName).append("type", "Semantic").append("IRI", datasetIRI);
		datasets.insertOne(dataset);
	}
}
