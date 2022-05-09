package com.spring.training.Controller;

import java.sql.SQLException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.spring.training.Service.CustomerService;

@Controller

public class CustomerController {
	@Autowired
	CustomerService customerService;
	
	//update Job Status by job Id
	
	@PutMapping(value = "/api/update-job/{jobId}/{userId}",produces = "application/json")
	public ResponseEntity<String> updateStatus(@RequestBody String status, @PathVariable Integer jobId,@PathVariable Integer userId)
			throws JsonMappingException, JsonProcessingException, SQLException {
		final HttpHeaders httpHeaders= new HttpHeaders();
	    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
		String map = customerService.updateStatus(status, jobId,userId);
		return new ResponseEntity<String>(map.toString(), httpHeaders,HttpStatus.OK);
	}
	
	
	//Filter API
	
	@PostMapping(value = "/api/filter/jobs",produces = "application/json")
	public ResponseEntity<String> filterJobs(@RequestBody String filter)
			throws JsonMappingException, JsonProcessingException, SQLException {
		final HttpHeaders httpHeaders= new HttpHeaders();
	    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
		String map = customerService.filterJobs(filter);
		return new ResponseEntity<String>(map.toString(), httpHeaders,HttpStatus.OK);
	}
	

	//delete multiple jobs by job Id
	
	@DeleteMapping("/api/delete/jobs")
	public ResponseEntity<String> deleteJobs(@RequestBody String jobIds)
			throws SQLException, JsonMappingException, JsonProcessingException {
		String resultList = customerService.deleteJobs(jobIds);
		return new ResponseEntity<String>(resultList.toString(), HttpStatus.OK);
	}
	
	
	//fetch Job Details by job Id
	
	@GetMapping(value = "/api/job/{jobId}",produces = "application/json")
	public ResponseEntity<String> getJob(@PathVariable Integer jobId) throws SQLException {
		final HttpHeaders httpHeaders= new HttpHeaders();
	    httpHeaders.setContentType(MediaType.APPLICATION_JSON);
		String resultMap = customerService.getJob(jobId);
		return new ResponseEntity<String>(resultMap.toString(), httpHeaders, HttpStatus.OK);
	}
	
	
}
