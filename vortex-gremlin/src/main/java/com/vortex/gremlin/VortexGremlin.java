package com.vortex.gremlin;

import com.vortex.gremlin.util.YAMLConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class VortexGremlin {

	@Autowired
	private YAMLConfig yamlConfig;

	public static void main(String[] args) {
		SpringApplication.run(VortexGremlin.class, args);
	}

}
