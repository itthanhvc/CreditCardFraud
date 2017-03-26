/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sparkSQL;

/** Java Bean class to be used with the example Transaction. */
public class Transaction implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int transId;

	public int getTransactionId() {
		return transId;
	}

	public void setTransactionId(int transId) {
		this.transId = transId;
	}

	private String accNo;

	public String getAccountNo() {
		return accNo;
	}

	public void setAccountNo(String accId) {
		this.accNo = accId;
	}

	private String transactionTime;

	public String getTime() {
		return transactionTime;
	}

	public void setTime(String transactionTime) {
		this.transactionTime = transactionTime;
	}

	private String location;

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	private float amount;

	public float getAmount() {
		return amount;
	}

	public void setAmount(float amount) {
		this.amount = amount;
	}
	private String alert;
	public String getAlert() {
		return alert;
	}

	public void setAlert(String alert) {
		this.alert = alert;
	}

	public Transaction(int transId, String accNo, String transactionTime,
			String location, float amount, String alert) {
		this.transId = transId;
		this.accNo = accNo;
		this.transactionTime = transactionTime;
		this.location = location;
		this.amount = amount;
		this.alert = alert;
	}
}