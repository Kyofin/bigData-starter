package com.wugui.sparkstarter.es;

import java.io.Serializable;

public class TripBean implements Serializable {
        private String departure, arrival;

        public TripBean(String departure, String arrival) {
            setDeparture(departure);
            setArrival(arrival);
        }

        public TripBean() {
        }

        public String getDeparture() {
            return departure;
        }

        public String getArrival() {
            return arrival;
        }

        public void setDeparture(String dep) {
            departure = dep;
        }

        public void setArrival(String arr) {
            arrival = arr;
        }
    }