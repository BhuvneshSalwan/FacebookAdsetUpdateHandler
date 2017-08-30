package com.hibernate.campaign.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "fb_city_code")
public class FBCityCode {

	@Id
	@Column(name = "id")
	private int id;

	@Column(name = "country")
	private String country;

	@Column(name = "region")
	private String region;

	@Column(name = "city")
	private String city;

	@Column(name = "city_codes")
	private String city_codes;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCity_codes() {
		return city_codes;
	}

	public void setCity_codes(String city_codes) {
		this.city_codes = city_codes;
	}

}