package org.example.company;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Company {

    @JsonProperty
    private int tradeNumber;
    @JsonProperty
    private  String registeredName;

    public Company() {}

    public Company(int tradeNumber, String registeredName) {
        this.tradeNumber = tradeNumber;
        this.registeredName = registeredName;
    }

    public Company(String registeredName) { this.registeredName = registeredName;}

    public int getTradeNumber() { return tradeNumber;}

    public void setTradeNumber(int tradeNumber) {
        this.tradeNumber = tradeNumber;
    }

    public String getRegisteredName() {
        return registeredName;
    }

    public void setRegisteredName(String registeredName) {
        this.registeredName = registeredName;
    }
}
