package net.thornydev;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

/**
 * Generates Hive schemas for use with the JSON SerDe from
 * org.openx.data.jsonserde.JsonSerDe.  GitHub link: https://github.com/rcongiu/Hive-JSON-Serde
 * 
 * Pass in a valid JSON document string to {@link JsonHiveSchema#createHiveSchema} and it will
 * return a Hive schema for the JSON document.
 * 
 * It supports embedded JSON objects, arrays and the standard JSON scalar types: strings,
 * numbers, booleans and null.  You probably don't want null in the JSON document you provide
 * as Hive can't use that.  For numbers - if the example value has a decimal, it will be 
 * typed as "double".  If the number has no decimal, it will be typed as "int".
 * 
 * This program uses the JSON parsing code from json.org and that code is included in this
 * library, since it has not been packaged and made available for maven/ivy/gradle dependency
 * resolution.
 * 
 * <strong>Use of main method:</strong> <br>
 *   JsonHiveSchema has a main method that takes a file path to a JSON doc - this file should have
 *   only one JSON file in it.  An optional second argument can be provided to name the Hive table
 *   that is generated.
 */
public class JsonHiveSchema  {
	static final String RESPONSE_ROOT = "Response";
	static final String KEYED_RESPONSE_ROOT = "KeyedResponse";
	static final String[] KEYED_RESPONSE_DATA = {"TPSSourceRecord","ApplicationData","keyData"};
	static final String XPATH_SERDE = "column.xpath.";
	static final String JSON_SERDE = "ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe';";
	static final String XML_SERDE = "ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'";
	static boolean skipKeyedResponse;
	
	static void help(Options options) {
	  HelpFormatter formatter = new HelpFormatter();
	  formatter.printHelp( "java -jar hive_schema.jar -input <input_file_path> -output <output_file_path> -inputType [ XML | JSON ] -tableName [table_name] -typePaths [rootTag/tag/object,rootTag/tag/anotherObject] ", options);
	}
	
	public enum HIVE_TYPE {
		primitive,
		struct,
		array,
		map
	}
  
  public static void main( String[] args ) throws Exception {
	  
	try {
		Options options = new Options();
		options.addOption("help","Print this message.");
		options.addOption("inputType",true,"Inform schema tool what type of file is being input. Defaults to 'JSON'.");
		options.addOption("tableName",true,"Tablename for the hive schema. Defaults to 'hive_table'.");
		options.addOption("input",true,"File to build schema from.");
		options.addOption("output",true,"File to output schema to.");
		options.addOption("skipKeyedResponse", false, "Skips the inclusion of the KeyedResponse tags in the XML. Defaults to false.");
		options.addOption("typePaths", true, "Paths to define independent primitive & complex types. Defaults to the root Response object.");

    	CommandLineParser parser = new DefaultParser();
    	CommandLine cmd = parser.parse(options,args);
    	
    	// Validate arguments and minimum options
    	if(cmd.hasOption("help")){
    		help(options);
    		System.exit(0);
    	}
    	else if (!cmd.hasOption("input") && !cmd.hasOption("output")) {
    		System.out.println("ERROR: Input & Output files must be specified.");
    		help(options);
    		System.exit(0);
    	}
    	
    	// Set defaults for execution
    	String inputFile = cmd.getOptionValue("input");
    	String outputFile = cmd.getOptionValue("output");
    	String tableName = (cmd.hasOption("tableName") ? cmd.getOptionValue("tableName") : "hive_table");
    	boolean convertXML = (cmd.hasOption("inputType") ? (cmd.getOptionValue("inputType").equalsIgnoreCase("XML") ? true : false) : false);
    	skipKeyedResponse = (cmd.hasOption("skipKeyedResponse") ? true : false);
    	String typePaths = (cmd.hasOption("typePaths") ? cmd.getOptionValue("typePaths").toString() : "Response");
    	String fileText = "";
    	String jsonText = "";
        JsonHiveSchema schemaWriter = new JsonHiveSchema(tableName);
        
        // Convert Nested Levels to a collection to iterate.
        List<String> primaryHiveTypes;
        
        if(typePaths.contains(",")){
        	primaryHiveTypes = Arrays.asList(typePaths.split(","));
        }
        else{
        	primaryHiveTypes = Arrays.asList(typePaths);
        }
        
    	// Read in the input file
        StringBuilder sb = new StringBuilder();
        BufferedReader br = new BufferedReader( new FileReader(inputFile) );
        String line;
        while ( (line = br.readLine()) != null ) {
          sb.append(line).append("\n");
        }
        br.close();
        
        fileText = sb.toString();
        
        // Manipulate JSONObject to create template object of Hive structure
        JSONObject initialObj;
        JSONObject finalObj;
        JSONObject respRoot;

        if(convertXML) {
        	schemaWriter.serdeType = "XML";
        	
        	// Convert the XML to a JSON Object
        	initialObj = XML.toJSONObject(fileText);

        	// Add the Keyed Data to the new object
        	if(!skipKeyedResponse) {
	        	for(String item : Arrays.asList(KEYED_RESPONSE_DATA)){
	            	schemaWriter._xPaths.add(formatXPath(item, item.replace("/", "_"), HIVE_TYPE.struct));        		
	        	}       	
        	}
        }
        else
        {
    		initialObj = new JSONObject(fileText);
        }

        if(!skipKeyedResponse) {
	       	// Take the known parts of the Key Data and create a new object that will be the final output
	    	finalObj = new JSONObject(initialObj.getJSONObject(KEYED_RESPONSE_ROOT), KEYED_RESPONSE_DATA );
	
	    	// Get the root response object, list of all keys, then determine if we iterate for certain sub-keys or type the whole response.
	    	respRoot = initialObj.getJSONObject(KEYED_RESPONSE_ROOT).getJSONObject(RESPONSE_ROOT);
        }
        else {
        	finalObj = new JSONObject(initialObj);
        	respRoot = initialObj.getJSONObject(RESPONSE_ROOT);
        }

        // Testing method for all keys
    	ArrayList<String> keys = schemaWriter.getKeys(respRoot);
    	keys.size();
        
    	for(String type : primaryHiveTypes){
    		if(type.equalsIgnoreCase("Response")){
    			finalObj.put(type, respRoot);
    		}
    		else if(type.contains("/")){
				Object o;
				String queryPath = "";
				if(type.contains("@")) {
					// Our type is an array that needs to be parsed as independent objects
					queryPath = type.split("@")[0];
					String key = type.split("@")[1].split(":")[0];
					String value = type.split("@")[1].split(":")[1];
					o = respRoot.query((!queryPath.startsWith("/")? "/" + queryPath : queryPath));
					String xpathValue = "";
					for(int i = 0; i < ((JSONArray)o).length(); i++) {
						JSONObject j = ((JSONArray)o).getJSONObject(i);
						if(j.get(key).equals(value)) {
							o = j;
							xpathValue = "[@" + key + "='" + value + "']";
							break;
						}
					}
					finalObj.put(queryPath.replace("/", "_") + "_" + value,o);
					if(convertXML){
						HIVE_TYPE hiveType;
						if(o instanceof JSONObject) {
							hiveType = HIVE_TYPE.struct;
						}
						else if(o instanceof JSONArray){
							hiveType = HIVE_TYPE.array;
						}
						else {
							hiveType = HIVE_TYPE.primitive;
						}
						schemaWriter._xPaths.add(formatXPath(RESPONSE_ROOT + "/" + queryPath + xpathValue, queryPath.replace("/", "_") + "_" + value, hiveType));
					}
				}
				else {
					queryPath = type;
					o = respRoot.query((!queryPath.startsWith("/")? "/" + queryPath : queryPath));
					finalObj.put(queryPath.replace("/", "_"),o);
					if(convertXML){
						HIVE_TYPE hiveType;
						if(o instanceof JSONObject) {
							hiveType = HIVE_TYPE.struct;
						}
						else if(o instanceof JSONArray){
							hiveType = HIVE_TYPE.array;
						}
						else {
							hiveType = HIVE_TYPE.primitive;
						}
						schemaWriter._xPaths.add(formatXPath(RESPONSE_ROOT + "/" + queryPath, queryPath.replace("/", "_"), hiveType));
					}
				}
    		}
    		else {
    			throw new IllegalArgumentException("Invalid path specified.");
    		}
    	}
    	
    	jsonText = finalObj.toString(); 
        
    	// Use the schema writer to build out DDL & write to console/file
        StringBuilder output = new StringBuilder();
        output.append(schemaWriter.createHiveSchema(jsonText)).append("\n").append(schemaWriter.createHiveQuery(jsonText));
        //output.append("\n\n\n\n\n").append(initialObj.toString(3));
        System.out.println(output.toString());
        FileWriter writer = null;
        try {
        	writer = new FileWriter(outputFile);
        	writer.write(output.toString());
        }
        finally {
        	if(writer != null){
        		writer.flush();
        		writer.close();
        	}
        }
	}
	finally {}
  }
  
  private String tableName = "hive_table";
  private String serdeType = "JSON";
  private ArrayList<String> _xPaths = new ArrayList<String>();
  private ArrayList<String> _reservedKeywords = new ArrayList<String>();
  
  public JsonHiveSchema() {}
  
  public JsonHiveSchema(String tableName) {
    this.tableName = tableName;
    _reservedKeywords.addAll(Arrays.asList(new String[] {"ALL","ALTER","AND","ARRAY","AS","AUTHORIZATION","BETWEEN","BIGINT","BINARY","BOOLEAN","BOTH","BY","CASE","CAST","CHAR","COLUMN","CONF","CREATE","CROSS","CUBE","CURRENT","CURRENT_DATE","CURRENT_TIMESTAMP","CURSOR","DATABASE","DATE","DECIMAL","DELETE","DESCRIBE","DISTINCT","DOUBLE","DROP","ELSE","END","EXCHANGE","EXISTS","EXTENDED","EXTERNAL","FALSE","FETCH","FLOAT","FOLLOWING","FOR","FROM","FULL","FUNCTION","GRANT","GROUP","GROUPING","HAVING","IF","IMPORT","IN","INNER","INSERT","INT","INTERSECT","INTERVAL","INTO","IS","JOIN","LATERAL","LEFT","LESS","LIKE","LOCAL","MACRO","MAP","MORE","NONE","NOT","NULL","OF","ON","OR","ORDER","OUT","OUTER","OVER","PARTIALSCAN","PARTITION","PERCENT","PRECEDING","PRESERVE","PROCEDURE","RANGE","READS","REDUCE","REGEXP","REVOKE","RIGHT","RLIKE","ROLLUP","ROW","ROWS","SELECT","SET","SMALLINT","TABLE","TABLESAMPLE","THEN","TIMESTAMP","TO","TRANSFORM","TRIGGER","TRUE","TRUNCATE","UNBOUNDED","UNION","UNIQUEJOIN","UPDATE","USER","USING","VALUES","VARCHAR","WHEN","WHERE","WINDOW","WITH"}));
  }
  
  /**
   * Pass in any valid JSON object and a Hive schema will be returned for it.
   * You should avoid having null values in the JSON document, however.
   * 
   * The Hive schema columns will be printed in alphabetical order - overall and
   * within subsections.
   * 
   * @param json
   * @return string Hive schema
   * @throws JSONException if the JSON does not parse correctly
   */
  public String createHiveSchema(String json) throws JSONException {
    JSONObject jo = new JSONObject(json);
    
    Iterator<String> keys = jo.keys();
    StringBuilder columnDDL = new StringBuilder();
    ArrayList<String> columns = new ArrayList<String>();
    columnDDL.append("%%");
	while (keys.hasNext()) {
		StringBuilder column = new StringBuilder();
		String k = keys.next();
		column.append("\t,");
		column.append((_reservedKeywords.contains(k.toUpperCase()) ? "`" + k.replace(":", "_").toLowerCase() + "`" : k.replace(":", "_").toLowerCase()));
		column.append(' ');
		column.append(valueToHiveSchema(jo.opt(k), k));
		column.append("\n");
		
		columns.add(column.toString());
	}
	Collections.sort(columns);
	for(String column : columns){
		columnDDL.append(column);
	}
	String columnDDLText = columnDDL.toString().replace("%%\t,","\t"); // remove first comma
    return formatHiveTable(tableName, columnDDLText);
  }
  
  public String createHiveQuery(String json) throws JSONException {
	  JSONObject jo = new JSONObject(json);
	    
	    Iterator<String> keys = jo.keys();
	    StringBuilder sb = new StringBuilder("CREATE VIEW view_name AS SELECT\n");
	    ArrayList<String> columns = new ArrayList<String>();
	    sb.append("%%");
	    while (keys.hasNext()) {
	    	StringBuilder column = new StringBuilder();
			String k = keys.next();
			column.append(valueToHiveQuery(jo.opt(k),k));
			columns.add(column.toString());
	    }
		Collections.sort(columns);
		for(String column : columns){
			sb.append(column);
		}	
	    
	    return sb.append("FROM ").append(tableName).append(" \n").toString().replace("%%\t,","\t"); // remove first comma
  }

  private String toHiveSchema(JSONObject o, String parent) throws JSONException { 
    Iterator<String> keys = o.keys();
    StringBuilder sb = new StringBuilder("struct<");
    
    while (keys.hasNext()) {
      String k = keys.next(); 
      String fieldName = (k.equals("content") ? parent : k);
      sb.append((_reservedKeywords.contains(fieldName.toUpperCase()) ? "`" + fieldName.replace(":", "_") + "`" : fieldName.replace(":", "_")));
      sb.append(':');
      sb.append(valueToHiveSchema(o.opt(k), k));
      sb.append(", ");
    }
    sb.replace(sb.length() - 2, sb.length(), ">"); // remove last comma
    return sb.toString();
  }
  
  private String toHiveSchema(JSONArray a, String parent) throws JSONException {
	    return "array<" + arrayJoin(a, ",", parent) + '>';
  }
 
  private String arrayJoin(JSONArray a, String separator, String parent) throws JSONException {
	    StringBuilder sb = new StringBuilder();

	    if (a.length() == 0) {
	      throw new IllegalStateException("Array is empty: " + a.toString());
	    }
	      
	    Object entry0 = a.get(0);
	    if ( isScalar(entry0) ) {
	      sb.append(scalarType(entry0));
	    } else if (entry0 instanceof JSONObject) {
	      sb.append(toHiveSchema((JSONObject)entry0, parent));
	    } else if (entry0 instanceof JSONArray) {    
	      sb.append(toHiveSchema((JSONArray)entry0, parent));
	    }
	    return sb.toString();
  }
  
  private String toHiveQuery(JSONObject o, String keyName) throws JSONException { 
	    Iterator<String> keys = o.keys();
	    StringBuilder sb = new StringBuilder();
	    
	    while (keys.hasNext()) {
	      String k = keys.next();
	      sb.append("\t,").append(keyName.toLowerCase()).append(".").append(k.toString().toLowerCase()); // first part
	      sb.append(" AS ").append(keyName.toLowerCase()).append("_").append(k.toLowerCase()).append("\n"); // AS part
	    }
	    
	    return sb.toString();
	  }
  
  private String scalarType(Object o) {
    if (o instanceof String) return "string";
    if (o instanceof Number) return scalarNumericType(o);
    if (o instanceof Boolean) return "boolean";
    return "string";
  }

  private String scalarNumericType(Object o) {
    String s = o.toString();
    if (s.indexOf('.') > 0) {
      return "double";
    } else {
      return "int";
    }
  }

  private boolean isScalar(Object o) {
    return o instanceof String ||
        o instanceof Number ||
        o instanceof Boolean || 
        o == JSONObject.NULL;
  }

  private String valueToHiveSchema(Object o, String parent) throws JSONException {
    if ( isScalar(o) ) {
      return scalarType(o);
    } else if (o instanceof JSONObject) {
      return toHiveSchema((JSONObject)o, parent);
    } else if (o instanceof JSONArray) {
      return toHiveSchema((JSONArray)o, parent);
    } else {
      throw new IllegalArgumentException("unknown type: " + o.getClass());
    }
  }
  
  private String valueToHiveQuery(Object o, String keyName) throws JSONException {
	  if (isScalar(o)) {
		  StringBuilder sb = new StringBuilder("\t,");
		  return sb.append(keyName.toLowerCase()).append(" AS ").append(keyName.toLowerCase().replace(".", "_")).append("\n").toString();
	  } else if (o instanceof JSONObject) {
		  return toHiveQuery((JSONObject)o,keyName);
	  } else if (o instanceof JSONArray) {
		  StringBuilder sb = new StringBuilder("\t,");
		  return sb.append(keyName.toLowerCase()).append(" AS ").append(keyName.toLowerCase().replace(".", "_")).append("\n").toString();
	  } else {
		  throw new IllegalArgumentException("unknown type: " + o.getClass());
	  }
  }
   
  private ArrayList<String> getKeys(Object o)
  {
	  ArrayList<String> keys = new ArrayList<String>();
	  if(o instanceof JSONObject){
		    Iterator<String> objKeys = ((JSONObject)o).keys();
		    while(objKeys.hasNext()){
		    	String key = objKeys.next();
		    	keys.add(key);
		    	if(((JSONObject)o).get(key) instanceof JSONObject){
		    		keys.addAll(getKeys(((JSONObject)o).get(key)));
		    	}
		    	else if(((JSONObject)o).get(key) instanceof JSONArray){
		    		JSONArray array = ((JSONObject)o).getJSONArray(key);
		    		for(int i = 0; i < array.length(); i++) {
		    			keys.addAll(getKeys(array.optJSONObject(i)));
		    		}
		    	}
		    }
	  }
	  
	  return keys;
  }
  
  private ArrayList<String> getArrayValueTypes(Object o)
  {  
	  ArrayList<String> valueTypes = new ArrayList<String>();
	  if(o instanceof JSONArray) {
		  for(int i = 0; i < ((JSONArray)o).length(); i++) {
			  valueTypes.addAll(getArrayValueTypes(((JSONArray)o).get(i)));
		  }
	  }
	  else if(o instanceof JSONObject){
		    Iterator<String> objKeys = ((JSONObject)o).keys();
		    while(objKeys.hasNext()){
		    	String key = objKeys.next();
		    	if(((JSONObject)o).get(key) instanceof JSONObject){
		    		valueTypes.addAll(getArrayValueTypes(((JSONObject)o).get(key)));
		    	}
		    	else if(((JSONObject)o).get(key) instanceof JSONArray){
		    		JSONArray array = ((JSONObject)o).getJSONArray(key);
		    		for(int i = 0; i < array.length(); i++) {
		    			valueTypes.addAll(getArrayValueTypes(array.get(i)));
		    		}
		    	}
		    	else if (isScalar(((JSONObject)o).get(key))){
		    		valueTypes.add(scalarType(((JSONObject)o).get(key)));
		    	}
		    }
	  }
	  else if (isScalar(o)){
		  valueTypes.add(scalarType(o));
	  }
	  
	  return valueTypes;
  }
  
  private String formatHiveTable(String tableName, String columnDDL){
	  StringBuilder sb = new StringBuilder("CREATE EXTERNAL TABLE ").append(tableName).append(" (\n").append(columnDDL).append(")\n");
	  sb.append("COMMENT 'Auto Generated Schema, Put Table description here'\n");
	  sb.append("PARTITIONED BY (CYCLE_NUMBER INT)\n");
	  if(serdeType.equalsIgnoreCase("XML")){
		  sb.append(XML_SERDE).append("\nWITH SERDEPROPERTIES (\n%%");
		  for(String xpath : _xPaths){
			  sb.append("\t,").append(xpath).append("\n");
		  }
		  // Append the rest of the table info
		  String xmlRoot = (skipKeyedResponse ? RESPONSE_ROOT : KEYED_RESPONSE_ROOT);
          sb.append(")\nSTORED AS\n").append("INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'\n")
              .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'\n")
              .append("TBLPROPERTIES (\n").append("\t\"xmlinput.start\"=\"<" + xmlRoot + "\",").append("\n")
              .append("\t\"xmlinput.end\"=\"</" + xmlRoot +">\"").append("\n);");
	  }
	  else {
		  sb.append(JSON_SERDE);
	  }
	  return sb.toString().replace("%%\t,", "\t"); //remove the first comma in the Xpaths.
  }
  
  private static String formatXPath(String path,String name,HIVE_TYPE type){
	  String xpath = (skipKeyedResponse ? "/" : KEYED_RESPONSE_ROOT) + path;
	  switch(type){
	  	case primitive:
	  		return String.format("\"%1$s%2$s\"=\"%3$s%4$s\"",XPATH_SERDE,name.toLowerCase(),xpath,"/text()");
	  	case struct:
	  		return String.format("\"%1$s%2$s\"=\"%3$s%4$s\"",XPATH_SERDE,name.toLowerCase(),xpath,"");
	  	case array:
	  		return String.format("\"%1$s%2$s\"=\"%3$s%4$s\"",XPATH_SERDE,name.toLowerCase(),"/",xpath);
		case map:
			return null;
		default:
			return null;
	  }	  
  }
}