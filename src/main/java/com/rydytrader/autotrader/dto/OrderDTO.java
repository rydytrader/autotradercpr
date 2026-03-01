package com.rydytrader.autotrader.dto;

public class OrderDTO {

    private String status;
    private String id;
    private String message;
    

    public  OrderDTO(String status, String id, String message ) {
        this.status = status;
        this.id = id;
        this.message = message;
    }

    public String getStatus() { return status; }
    public String getId() { return id; }
    public String getMessage() { return message; }


}
