package org.springframework.data.cassandra.test.integration.repository;

public class Address {

    String country;
    String state;
    String city;
    String street;
    String streetNo;
    String flatNo;
    String postalCode;

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getStreetNo() {
        return streetNo;
    }

    public void setStreetNo(String streetNo) {
        this.streetNo = streetNo;
    }

    public String getFlatNo() {
        return flatNo;
    }

    public void setFlatNo(String flatNo) {
        this.flatNo = flatNo;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    @Override
    public String toString() {
        return "Address [" +
               (country != null ? "country=" + country + ", " : "") +
               (state != null ? "state=" + state + ", " : "") +
               (city != null ? "city=" + city + ", " : "") +
               (street != null ? "street=" + street + ", " : "") +
               (streetNo != null ? "streetNo=" + streetNo + ", " : "") +
               (flatNo != null ? "flatNo=" + flatNo + ", " : "") +
               (postalCode != null ? "postalCode=" + postalCode : "") +
               "]";
    }

}
