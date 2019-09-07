package com.fms.graph.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestMainCtrl {
	
	private static Logger logger = LoggerFactory.getLogger(RestMainCtrl.class);
	
	@RequestMapping(value = "/version", method= RequestMethod.GET)
	public String version() {
		logger.info(">>>>>>>>> test <<<<<<<");
		return "FMS Graph Version 1";
	}
}
