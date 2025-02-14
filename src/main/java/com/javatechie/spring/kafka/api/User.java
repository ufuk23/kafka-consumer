package com.javatechie.spring.kafka.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {

	private int id;
	private String name;
	private String[] address;

	public String toString(){
		return this.id + " " + this.name;
	}

}
