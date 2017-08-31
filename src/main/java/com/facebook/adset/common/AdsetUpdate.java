package com.facebook.adset.common;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableRow;
import com.google.bigquery.main.BQOperations;
import com.hibernate.campaign.entities.FBCityCode;
import com.hibernate.campaign.main.DBOperations;

public class AdsetUpdate {

	private static final String URL = "https://graph.facebook.com";
	private static final String VERSION = "v2.9";
	private static final String SESSION_TOKEN = "CAAWXmQeQZAmcBANADF6ew1ZBXAAifj7REIcHmbTVjkAR5q6GAnRjrpcuVhhV435LHMXpb8HzUKzQaUU4uwkxIl5xpYSgzUNog43JX4qxe0pqVBvjHZCsPfgIpRRGY7xfFC2hb1Hi1s9EH0IhQu4KlnTGcsdgIq5FN2ufeNHOeEB9YGck36aah1rPHrdi10ZD";
	
	public static Boolean getCreateResults(TableRow row, Bigquery bigquery){
		
		try{
			
			String adset_id = null, adset_name = null, account_id = null, campaign_id = null, daily_budget = null, status = null, age_min = null, age_max = null, genders = null, cities = null, countries = null, audiences = null, pixel_id = null, parse_client_id = null, hostname = null, flag_budget = null, optimization_event = null, publisher_platforms = null, device_platforms = null, optimization_goal = null, targeting_optimization = null;
			
			try{ adset_name = (String) row.getF().get(0).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Adset Name i.e. " + row.getF().get(0).getV() + "."); return false;}
			try{ account_id = (String) row.getF().get(1).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Account_ID i.e. " + row.getF().get(1).getV() + "."); return false;}
			try{ campaign_id = (String) row.getF().get(2).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Campaign_ID i.e. " + row.getF().get(2).getV() + "."); return false;}
			try{ daily_budget = (String) row.getF().get(3).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Daily_Budget i.e. " + row.getF().get(3).getV() + "."); return false;}
			try{ status = (String) row.getF().get(4).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for Status i.e. " + row.getF().get(4).getV() + "."); return false;}
			try{ age_min = (String) row.getF().get(5).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for age_min i.e. " + row.getF().get(5).getV() + "."); age_min = "0";}
			try{ age_max = (String) row.getF().get(6).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for age_max i.e. " + row.getF().get(6).getV() + "."); age_max = "0";}
			try{ genders = (String) row.getF().get(7).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for genders i.e. " + row.getF().get(7).getV() + "."); genders = "0";}
			try{ cities = (String) row.getF().get(8).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for cities i.e. " + row.getF().get(8).getV() + "."); cities = "null";}
			try{ countries = (String) row.getF().get(9).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for countries i.e. " + row.getF().get(9).getV() + "."); countries = "null";}
			try{ audiences = (String) row.getF().get(10).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for audiences i.e. " + row.getF().get(10).getV() + "."); audiences = "null";}
			try{ pixel_id = (String) row.getF().get(11).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for pixel_id i.e. " + row.getF().get(11).getV() + "."); pixel_id = "null";}
			try{ parse_client_id = (String) row.getF().get(12).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for parse_client_id i.e. " + row.getF().get(12).getV() + "."); parse_client_id = "null";}
			try{ hostname = (String) row.getF().get(13).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for hostname i.e. " + row.getF().get(13).getV() + "."); hostname = "null";}
			try{ flag_budget = (String) row.getF().get(14).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for flag_budget i.e. " + row.getF().get(14).getV() + "."); parse_client_id = "null";}
			try{ adset_id = (String) row.getF().get(15).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for adset_id i.e. " + row.getF().get(15).getV() + "."); parse_client_id = "null";}
			try{ optimization_event = (String) row.getF().get(16).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for optimization_event i.e. " + row.getF().get(16).getV() + "."); optimization_event = "null";}
			try{ publisher_platforms = (String) row.getF().get(17).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for publisher_platforms i.e. " + row.getF().get(17).getV() + "."); publisher_platforms = "null";}
			try{ device_platforms = (String) row.getF().get(18).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for device_platforms i.e. " + row.getF().get(18).getV() + "."); device_platforms = "null";}
			try{ optimization_goal = (String) row.getF().get(19).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for optimization_goal i.e. " + row.getF().get(19).getV() + "."); optimization_goal = "null";}
			try{ targeting_optimization = (String) row.getF().get(20).getV(); }catch(Exception e){ System.out.println("Exception while processing the value for targeting_optimization i.e. " + row.getF().get(20).getV() + "."); targeting_optimization = "null";}
			
	    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String custom_url = URL + "/" + VERSION + "/" + adset_id;
			
			HttpClient reqclient = new DefaultHttpClient();
			HttpPost reqpost = new HttpPost(custom_url);
			
			ArrayList<NameValuePair> parameters = new ArrayList<NameValuePair>();
			parameters.add(new BasicNameValuePair("access_token",SESSION_TOKEN));
			
			if(null !=  daily_budget && !daily_budget.equals("0") && !daily_budget.equals("null")){
				//	parameters.add(new BasicNameValuePair("daily_budget", daily_budget));
			}
			
			JSONObject targeting = new JSONObject();
			
			JSONObject geoLocation = new JSONObject();
			
			if(cities.equals("") || cities.equals("null")){
				
				JSONArray country = new JSONArray();				
				country.put(countries);
	  			geoLocation.put("countries", country);
			
			}
			
			else{
				
				JSONObject jsonCities = new JSONObject(cities);
                
			    ArrayList<String> jsonCityCodes = new ArrayList<String>();
			                   
			    if(jsonCities != null){
		                		   
			         for(int arr_i = 1; arr_i <= 10; arr_i++){
	    		          
			        	 if(!(jsonCities.getString("city".concat(String.valueOf(arr_i))).equals(""))){
			                			   
			                	FBCityCode cityCode = (FBCityCode) DBOperations.getObject().getCityCodeByName(jsonCities.getString("city".concat(String.valueOf(arr_i))));
			                			     
			                	if(null != cityCode){
			                				   
			                	    jsonCityCodes.add(cityCode.getCity_codes());
			                			   
			                	}
			                			   
			              }
			                		   
	    		       }
		                		   
		          }
			                   
			      JSONArray arr = new JSONArray();
			      for (String cityCode : jsonCityCodes) {
			         JSONObject jsonObj = new JSONObject();
			         jsonObj.put("key", cityCode);
			         jsonObj.put("radius", 18);
			         jsonObj.put("distance_unit", "kilometer");
			         arr.put(jsonObj);
			      }

			      geoLocation.put("cities", arr);
				
			}
			
  			targeting.put("geo_locations", geoLocation);
				
			if(!age_min.equals(0)){
				targeting.put("age_min", age_min);
			}
			
			if(!age_max.equals(0)){
				targeting.put("age_max", age_max);
			}

			JSONArray gender = new JSONArray();
			
			if (!genders.equals("0")) {
					
					gender.put(genders);
					targeting.put("genders", gender);
				  
			}
			
			if(audiences.equals("1")){
				
				List<TableRow> rows = BQOperations.getAudienceInput(bigquery, parse_client_id, adset_name);
				
				if(null != rows){
					
					JSONArray custom_audiences = new JSONArray();

					JSONArray excluded_custom_audiences = new JSONArray();
					
					for(TableRow row_audience : rows){
						
						if(String.valueOf(row_audience.getF().get(1).getV()).equalsIgnoreCase("Include")){
							
							System.out.println("Adding this as Inclusions : " +String.valueOf(row_audience.getF().get(0).getV()));
							
							custom_audiences.put(new JSONObject().put("id", String.valueOf(row_audience.getF().get(0).getV())));
							
						}
						else{
							
							System.out.println("Adding this as Exclusions : " +String.valueOf(row_audience.getF().get(0).getV()));

							excluded_custom_audiences.put(new JSONObject().put("id", String.valueOf(row_audience.getF().get(0).getV())));
							
						}
						
					}
					
					if(custom_audiences.length() > 0){
						targeting.put("custom_audiences", custom_audiences.toString());
					}
					
					if(excluded_custom_audiences.length() > 0){
						targeting.put("excluded_custom_audiences", excluded_custom_audiences.toString());
					}
					
				}
				
			}
			
			targeting.put("targeting_optimization", targeting_optimization);
			targeting.put("publisher_platforms", new JSONArray(publisher_platforms));
			targeting.put("device_platforms", new JSONArray(device_platforms));

    //      	parameters.add(new BasicNameValuePair("billing_event","IMPRESSIONS"));
          	parameters.add(new BasicNameValuePair("name", adset_name));
          	parameters.add(new BasicNameValuePair("promoted_object","{\"pixel_id\":" + pixel_id + ",\"custom_event_type\":\"" + optimization_event + "\"}"));
    //      	parameters.add(new BasicNameValuePair("promoted_object","{\"pixel_id\":" + pixel_id + ",\"custom_event_type\":\"PURCHASE\"}"));
    //      	parameters.add(new BasicNameValuePair("optimization_goal", "OFFSITE_CONVERSIONS"));
          	parameters.add(new BasicNameValuePair("campaign_id", campaign_id));
            parameters.add(new BasicNameValuePair("targeting", targeting.toString()));
            
            String startDate = null;
            
            startDate = dateFormat.format(new Date(System.currentTimeMillis()));
            startDate = startDate.replace(' ', 'T');

    //        parameters.add(new BasicNameValuePair("start_time",startDate));
    //        parameters.add(new BasicNameValuePair("end_time", "0"));
    //        parameters.add(new BasicNameValuePair("is_autobid", "true"));
			parameters.add(new BasicNameValuePair("status", status));
			
			reqpost.setEntity(new UrlEncodedFormEntity(parameters));
			
			System.out.println("Sending POST request to URL : " + custom_url);
			System.out.println("POST Parameters : " + parameters);
			
			HttpResponse response = reqclient.execute(reqpost);
			
			System.out.println("Response Code : "+ response.getStatusLine().getStatusCode());

			StringBuffer buffer = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			
			String line = "";
			
			while((line = reader.readLine()) != null){
				buffer.append(line);
			}
			
			System.out.println(buffer.toString());
			
			if(response.getStatusLine().getStatusCode() >= 200 && response.getStatusLine().getStatusCode() < 300){
				
				JSONObject object = new JSONObject(buffer.toString());
				
				if(object.has("id")){
					return true;
				}
				else{
					System.out.println("Response Message : Request Failed for the Campaign as there is no ID returned by Facebook.");
					return false;
				}
				
			}
			else{
				System.out.println("Response Message : Request Failed for Adset Creation for Client with ID : " + parse_client_id + " and Adset Name : " + adset_name);
				return false;
			}
	
		}
		catch(Exception e){
			System.out.println(e);
			return false;
		}
		
	}
	
}