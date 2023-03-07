package org.fllik.geektime.flinksql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Userproduct {
    private Integer product_id;
    private String buyer_name;
    private Long date_time;
    private Double price;

}
