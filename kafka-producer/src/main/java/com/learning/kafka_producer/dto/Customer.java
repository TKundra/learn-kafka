package com.learning.kafka_producer.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class Customer {
    private int id;
    private String name;
    private String email;
    private String contactNo;
}
