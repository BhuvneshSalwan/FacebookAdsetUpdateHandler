package com.google.bigquery.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableRow;

public class BQOperations {
	
	private static final String PROJECT_ID = "stellar-display-145814";
	private static final String DATASET_ID = "table_output";
	
	public static void truncateTable(Bigquery bigquery, String TABLE_ID){
		try{
			bigquery.tables().delete(PROJECT_ID, DATASET_ID, TABLE_ID).execute();
		}
		catch(Exception e){
			System.out.println("EXCEPTION : BQOperations Class - truncateTable");
			System.out.println(e);
			System.out.println("Response Message : TABLE wasn't truncated.");
		}
	}

	public static Boolean StructureValidate(Bigquery bigquery, String TABLE_ID){
		try {
			TableList tables = bigquery.tables().list(PROJECT_ID, DATASET_ID).execute();	
			System.out.println(tables);
			if(null != tables && null != tables.getTables()){
				for(Tables table : tables.getTables()){
					if(table.getId().equalsIgnoreCase(PROJECT_ID + ":" + DATASET_ID + "." + TABLE_ID)){
						return true;
					}
				}
			}	
			return false;
		} catch (Exception e) {
			System.out.println(e);
			return false;	
		}
	}
	
	public static TableDataList GetUpdateRows(Bigquery bigquery, String TABLE_ID){
		try {
			TableDataList rows = bigquery.tabledata().list(PROJECT_ID, DATASET_ID, TABLE_ID).execute();
			if(rows.getTotalRows().intValue() != 0){
				return rows;
			}
			else{
				return null;
			}
		} catch (Exception e) {
			System.out.println(e);
			return null;
		}
	}
	
	   
	public static List<TableRow> getAudienceInput(Bigquery bigquery, String parse_client_id, String adset_name) throws IOException, InterruptedException, JSONException{
		
			String querysql = "SELECT audience_id, action FROM [stellar-display-145814:table_output.adset_audiences] where adset_name = \""+ adset_name +"\" and parse_client_id = \"" + parse_client_id + "\";";
			     
			JobReference jobId = startQuery(bigquery, PROJECT_ID, querysql);

			// Poll for Query Results, return result output
			Job completedJob = checkQueryResults(bigquery, PROJECT_ID, jobId);

			// Return and display the results of the Query Job
			return getQueryResults(bigquery, PROJECT_ID, completedJob);
    
	}

    public static JobReference startQuery(Bigquery bigquery, String projectId,
		                                        String querySql) throws IOException {
		    System.out.format("\nSelection Query Job: %s\n", querySql);

		    Job job = new Job();
		    JobConfiguration config = new JobConfiguration();
		    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
		    config.setQuery(queryConfig);

		    job.setConfiguration(config);
		    queryConfig.setQuery(querySql);

		    Insert insert = bigquery.jobs().insert(projectId, job);
		    insert.setProjectId(projectId);
		    JobReference jobId = insert.execute().getJobReference();

		    System.out.format("\nJob ID of Query Job is: %s\n", jobId.getJobId());

		    return jobId;
	}

    private static Job checkQueryResults(Bigquery bigquery, String projectId, JobReference jobId)
		      throws IOException, InterruptedException {
		    // Variables to keep track of total query time
		    long startTime = System.currentTimeMillis();
		    long elapsedTime;

		    while (true) {
		      Job pollJob = bigquery.jobs().get(projectId, jobId.getJobId()).execute();
		      elapsedTime = System.currentTimeMillis() - startTime;
		      System.out.format("Job status (%dms) %s: %s\n", elapsedTime,
		          jobId.getJobId(), pollJob.getStatus().getState());
		      if (pollJob.getStatus().getState().equals("DONE")) {
		        return pollJob;
		      }
		      Thread.sleep(1000);
		    }

    }

    private static List<TableRow> getQueryResults(Bigquery bigquery, String projectId, Job completedJob) {

    	try{
    	
    		GetQueryResultsResponse queryResult = bigquery.jobs()
		        .getQueryResults(
		            projectId, completedJob
		            .getJobReference()
		            .getJobId()
		        ).execute();
		    
		    int totRows = queryResult.getTotalRows().intValue();
		    
		    System.out.println("Total Rows fetched are : "+totRows);
		    
		    if(totRows > 0){
		    	
		    	return queryResult.getRows();
			    
		    }
		    
		    else{
		    	
		    	return null;
		    	
		    }
		    
    	}catch(Exception e){
    		
    		System.out.println(e);
    		
    		return null;
    		
    	}
		    
	}
	
}