package com.simple.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class News {
    private String title;

    private Integer popular_degree;

    private String source_class;
}
