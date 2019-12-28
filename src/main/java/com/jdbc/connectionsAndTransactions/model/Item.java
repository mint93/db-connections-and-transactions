package com.jdbc.connectionsAndTransactions.model;

import java.time.LocalDate;

import lombok.Data;

@Data
public class Item {
	private final Integer id;
	private final String name;
	private final LocalDate releaseDate;
	private final Integer version;
	
	public Item(String name) {
		this(name, LocalDate.now(), null);
	}
	
	public Item(String name, LocalDate releaseDate) {
		this(name, releaseDate, null);
	}
	
	public Item(String name, Integer version) {
		this(name, LocalDate.now(), version);
	}
	
	public Item(String name, LocalDate releaseDate, Integer version) {
		this(null, name, releaseDate, version);
	}
	
	public Item(Integer id, String name, LocalDate releaseDate, Integer version) {
		this.id = id;
		this.name = name;
		this.releaseDate = releaseDate;
		this.version = version;
	}
	
	public boolean optimisticLockingEnabled() {
		return getVersion() != null;
	}

}
