package org.example.chapter02;

/**
 * @Author: john
 * @Date: 2022-09-25-23:30
 * @Description:
 */
public class MyOrder {


    public Long id;
    public String product;
    public int amount;
    public MyOrder (){

    }

    public MyOrder(Long id, String product, int amount) {
        this.id = id;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "MyOrder{" +
                "id=" + id +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}
