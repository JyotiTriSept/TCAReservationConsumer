package com.example.demo.model;

public class ReservationCodeModel {

	private String pms_code;
	private String external_code;

	public ReservationCodeModel(String pms_code, String external_code) {
		super();
		this.pms_code = pms_code;
		this.external_code = external_code;
	}

	public String getPms_code() {
		return pms_code;
	}

	public void setPms_code(String pms_code) {
		this.pms_code = pms_code;
	}

	public String getExternal_code() {
		return external_code;
	}

	public void setExternal_code(String external_code) {
		this.external_code = external_code;
	}

}
