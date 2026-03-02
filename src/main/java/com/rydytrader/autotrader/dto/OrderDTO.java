package com.rydytrader.autotrader.dto;

/**
 * Data transfer object representing the result of a Fyers order placement or cancellation.
 */
public class OrderDTO {

    private String status;
    private String id;
    private String message;

    /**
     * Constructs an OrderDTO from a Fyers API response.
     *
     * @param status  response status string (e.g. "ok" or "error")
     * @param id      the Fyers order ID assigned by the exchange
     * @param message human-readable description or error message from Fyers
     */
    public  OrderDTO(String status, String id, String message ) {
        this.status = status;
        this.id = id;
        this.message = message;
    }

    /** Returns the Fyers response status ("ok" / "error"). */
    public String getStatus() { return status; }

    /** Returns the Fyers order ID assigned by the exchange. */
    public String getId() { return id; }

    /** Returns the human-readable message from the Fyers API response. */
    public String getMessage() { return message; }


}
