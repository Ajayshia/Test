package com.spring.training.Service;

import java.sql.SQLException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.training.Repository.CustomerRepository;

@Service

public class CustomerService {

	@Autowired
	CustomerRepository customerRepository;

	//update Job Status by job Id

	public String updateStatus(String status, Integer jobId, Integer userId)
			throws JsonMappingException, JsonProcessingException, SQLException {

		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = mapper.readValue(status, Map.class);
		String resultmap = customerRepository.updateStatus(map, jobId,userId);
		return resultmap;
	}
	
	//Filter API

	public String filterJobs(String filter)
			throws JsonMappingException, JsonProcessingException, SQLException {
		{

			ObjectMapper mapper = new ObjectMapper();
			Map<String, Object> map = mapper.readValue(filter, new TypeReference<Map<String, Object>>() {
			});

			String resultmap = customerRepository.filterJobs(map);
			return resultmap;
		}
	}
	

	//delete multiple jobs by job Id

	public String deleteJobs(String jobIds) throws JsonMappingException, JsonProcessingException, SQLException {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = mapper.readValue(jobIds, Map.class);
		String resultmap = customerRepository.deleteJobs(map);
		return resultmap;
	}


	//fetch Job Details by job Id
	
	public String getJob(Integer jobId) throws SQLException {
		String resultMap = customerRepository.getJob(jobId);
		return resultMap;
	}

	}

