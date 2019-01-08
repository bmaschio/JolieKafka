/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package joliekafkaconnector;

import java.io.Serializable;
import jolie.runtime.Value;

/**
 *
 * @author maschio
 */
public class CustomObject implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String id;
	private Value name;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Value getName() {
		return name;
	}

	public void setName(Value name) {
		this.name = name;
	}
}

