package com.spring.training.Repository;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.stream.Collectors;

@Repository
public class CustomerRepository {

	private JdbcTemplate jdbcTemplate;
	@Autowired
	public Environment env;

	@Inject
	public CustomerRepository(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

	//update Job Status by job Id

	public String updateStatus(Map<String, Object> map, Integer jobId, Integer userId) throws SQLException {

		int resultSet = 0;
		Connection connect = null;
		PreparedStatement preparedStatement = null;
		Map<String,String> status = new HashMap<String,String>();
		String json = null;

		try {
			connect = jdbcTemplate.getDataSource().getConnection();
         // user permission
	       Boolean role = fetchUserRoles(userId);
	       
	       String jobStatus = (String) map.get("status");
	       
	      if((role == false && !jobStatus.equalsIgnoreCase("kill") 
	    		  && jobStatus.equalsIgnoreCase("rerun")) || role == true) {
			String query = "UPDATE tbl_jobs SET Status=? WHERE JobId = ?;";

			preparedStatement = connect.prepareStatement(query);

			preparedStatement.setString(1, jobStatus);
			preparedStatement.setInt(2, jobId);

			resultSet = preparedStatement.executeUpdate();

			if (resultSet == 1) {
				status.put("message", "Status Updated Successfully");
			} else {
				status.put("message", "Failed to Update the Status");
			}
		}
	      else {
	    	  status.put("message", "No Permission");
	      }
	      ObjectMapper objectMapper = new ObjectMapper();
			json = objectMapper.writeValueAsString(status);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connect != null) {
				connect.close();
			}

			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}

		return json;
	}

	
   //Method to fetch user roles
	
	private Boolean fetchUserRoles(Integer userId) throws SQLException {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		String json = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;
		List<Integer> roleList = new ArrayList<>();
		Boolean access = false;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			String query = "select * from tbl_job_roles where UserId= ?;";

			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, userId);
			ResultSet resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				resultMap = new HashMap<String, Object>();
				roleList.add(resultSet.getInt("RoleId"));
				
				//Consider role id for admin access is 1
				Integer adminRole = 1;
				access = roleList.contains(adminRole);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}

			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}

		return access;
	}


	//Filter API
	
	public String filterJobs(Map<String, Object> map) throws SQLException {

		Connection connect = null;
		PreparedStatement preparedStatement = null;
		Map<String, Object> resultMap = new HashMap<String, Object>();
		String startDateTime,endDateTime,json = null,startString,endString = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Boolean flag = false;
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		java.sql.Date startDate,endDate= null;
		Long startDateInmilliseconds,endDateInmilliseconds = 0L;
		java.util.Date start,end = null;
		Integer count = 1;

		try {
			connect = jdbcTemplate.getDataSource().getConnection();

			if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null
					&& map.get("isActiveFlag") != "" && map.containsKey("startDate") && map.get("startDate") != null
					&& map.get("startDate") != "" && map.containsKey("endDate") && map.get("endDate") != null
					&& map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and IsActiveFlag=? and StartDate=? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				preparedStatement.setBoolean(2, (boolean) map.get("isActiveFlag"));
				
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(3, startDateTime);
				
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(4, endDateTime);
				
				

			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null
					&& map.get("isActiveFlag") != "" && map.containsKey("startDate") && map.get("startDate") != null
					&& map.get("startDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and IsActiveFlag=? and StartDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				preparedStatement.setBoolean(2, (boolean) map.get("isActiveFlag"));
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(3, startDateTime);
			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null
					&& map.get("isActiveFlag") != "" && map.containsKey("endDate") && map.get("endDate") != null
					&& map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and IsActiveFlag=? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				preparedStatement.setBoolean(2, (boolean) map.get("isActiveFlag"));
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(3, endDateTime);
				
			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("startDate") && map.get("startDate") != null && map.get("startDate") != ""
					&& map.containsKey("endDate") && map.get("endDate") != null && map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and StartDate=? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(2, startDateTime);
				
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(3, endDateTime);
			}

			else if (map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null && map.get("isActiveFlag") != ""
					&& map.containsKey("startDate") && map.get("startDate") != null && map.get("startDate") != ""
					&& map.containsKey("endDate") && map.get("endDate") != null && map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where IsActiveFlag=? and StartDate=? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setBoolean(1, (boolean) map.get("isActiveFlag"));
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(2, startDateTime);

				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(3, endDateTime);
			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null
					&& map.get("isActiveFlag") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and IsActiveFlag=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				preparedStatement.setBoolean(2, (boolean) map.get("isActiveFlag"));
			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("startDate") && map.get("startDate") != null && map.get("startDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and StartDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(2, startDateTime);
			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != ""
					&& map.containsKey("endDate") && map.get("endDate") != null && map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status= ? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(2, endDateTime);
			}

			else if (map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null && map.get("isActiveFlag") != ""
					&& map.containsKey("startDate") && map.get("startDate") != null && map.get("startDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where IsActiveFlag=? and StartDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setBoolean(1, (boolean) map.get("isActiveFlag"));
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(2, startDateTime);
			}

			else if (map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null && map.get("isActiveFlag") != ""
					&& map.containsKey("endDate") && map.get("endDate") != null && map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where IsActiveFlag=? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setBoolean(1, (boolean) map.get("isActiveFlag"));
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(2, endDateTime);
			}

			else if (map.containsKey("startDate") && map.get("startDate") != null && map.get("startDate") != ""
					&& map.containsKey("endDate") && map.get("endDate") != null && map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where StartDate=? and EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(1, startDateTime);
				
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(2, endDateTime);
			}

			else if (map.containsKey("status") && map.get("status") != null && map.get("status") != "") {
				flag = true;
				String query = "select * from tbl_jobs where Status=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setString(1, map.get("status").toString());

			}

			else if (map.containsKey("isActiveFlag") && map.get("isActiveFlag") != null
					&& map.get("isActiveFlag") != "") {
				flag = true;
				String query = "select * from tbl_jobs where IsActiveFlag=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				preparedStatement.setBoolean(1, (boolean) map.get("isActiveFlag"));
			}

			else if (map.containsKey("startDate") && map.get("startDate") != null && map.get("startDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where StartDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				startString = map.get("startDate").toString();
				start = sdf.parse(startString);
				startDateInmilliseconds = start.getTime();
				startDate = new java.sql.Date(startDateInmilliseconds);
				startDateTime = sdf.format(startDate);
				preparedStatement.setString(1, startDateTime);
			}

			else if (map.containsKey("endDate") && map.get("endDate") != null && map.get("endDate") != "") {
				flag = true;
				String query = "select * from tbl_jobs where EndDate=? ORDER BY StartDate ASC ;";

				preparedStatement = connect.prepareStatement(query);
				endString = map.get("endDate").toString();
				end = sdf.parse(endString);
				endDateInmilliseconds = end.getTime();
				endDate = new java.sql.Date(endDateInmilliseconds);
				endDateTime = sdf.format(endDate);
				preparedStatement.setString(1, endDateTime);
			}

			if (flag == true) {
				ResultSet resultSet = preparedStatement.executeQuery();

				while (resultSet.next()) {
					Map<String, Object> result = new HashMap<String, Object>();
					result.put("JobId", resultSet.getInt("JobId"));
					result.put("Description", resultSet.getString("Description"));
					result.put("Status", resultSet.getString("Status"));
					result.put("StartDate", resultSet.getDate("StartDate").toString() +" " + resultSet.getTime("StartDate").toString());
					result.put("EndDate", resultSet.getDate("EndDate").toString() +" " + resultSet.getTime("EndDate").toString());
					result.put("UpdatedDate", resultSet.getDate("UpdatedDate").toString() +" " + resultSet.getTime("UpdatedDate").toString());
					result.put("Errors", resultSet.getString("Errors"));
					result.put("IsActiveFlag", resultSet.getBoolean("IsActiveFlag"));
					resultList.add(result);

				}

				int pageSize = 10;
				AtomicInteger counter = new AtomicInteger();
				final Collection<List<Map<String, Object>>> partitionedList = resultList.stream()
						.collect(Collectors.groupingBy(i -> counter.getAndIncrement() / pageSize)).values();
				Map<Integer, Object> resultantMap = new HashMap<Integer, Object>();
				for (List<Map<String, Object>> subList : partitionedList) {

					resultantMap.put(count, subList);
					count++;
					ObjectMapper objectMapper = new ObjectMapper();
					json = objectMapper.writeValueAsString(resultantMap);
				}

			} else {
				Map<String, Object> result = new HashMap<String, Object>();
				result.put("message", "No data available for the particular filter");
				ObjectMapper objectMapper = new ObjectMapper();
				json = objectMapper.writeValueAsString(result);

			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connect != null) {
				connect.close();
			}

			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}

		return json;
	}
	
	
	//delete multiple jobs by job Id

	public String deleteJobs(Map<String, Object> map) throws SQLException {

		Connection connection = null;
		PreparedStatement preparedStatement = null;
		int resultSet = 0;
		String status = null;
		List<Integer> jobList = (List<Integer>) map.get("jobIds");
		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			if (jobList != null && jobList.size() > 0) {
				for (int i = 0; i < jobList.size(); i++) {

					String query = "DELETE FROM tbl_jobs WHERE JobId = ?";

					preparedStatement = connection.prepareStatement(query);
					preparedStatement.setInt(1, jobList.get(i));
					resultSet = preparedStatement.executeUpdate();

					if (resultSet == 1) {
						status = "Job(s) Deleted Successfully";
					} else {
						status = "Failed to Delete the job(s)";
					}
				}

			}

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}

			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}

		return status;

	}
	

    //fetch Job Details by job Id
	
	public String getJob(Integer jobId) throws SQLException {

		Map<String, Object> resultMap = new HashMap<String, Object>();
		String json = null;
		Connection connection = null;
		PreparedStatement preparedStatement = null;

		try {
			connection = jdbcTemplate.getDataSource().getConnection();

			String query = "select * from tbl_jobs where JobId= ?;";

			preparedStatement = connection.prepareStatement(query);
			preparedStatement.setInt(1, jobId);
			ResultSet resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				resultMap = new HashMap<String, Object>();

				resultMap.put("JobId", resultSet.getInt("JobId"));
				resultMap.put("Description", resultSet.getString("Description"));
				resultMap.put("Status", resultSet.getString("Status"));
				resultMap.put("StartDate", resultSet.getDate("StartDate").toString() +" " + resultSet.getTime("StartDate").toString());
				resultMap.put("EndDate", resultSet.getDate("EndDate").toString() +" " + resultSet.getTime("EndDate").toString());
				resultMap.put("UpdatedDate", resultSet.getDate("UpdatedDate").toString() +" " + resultSet.getTime("UpdatedDate").toString());
				resultMap.put("Errors", resultSet.getString("Errors"));
				resultMap.put("IsActiveFlag", resultSet.getBoolean("IsActiveFlag"));

				ObjectMapper objectMapper = new ObjectMapper();
				json = objectMapper.writeValueAsString(resultMap);

			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}

			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}

		return json;
	}
	
	
}
