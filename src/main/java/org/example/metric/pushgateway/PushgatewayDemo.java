package org.example.metric.pushgateway;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: john
 * @Date: 2022-12-13-16:52
 * @Description:
 */
public class PushgatewayDemo {


    public static void main(String[] args) {
        try {
            String url = "10.2.3.244:18165";
            CollectorRegistry registry = new CollectorRegistry();
            Gauge guage = Gauge.build("my_custom_metric11", "This is my custom metric.").create();
            guage.set(1.22);
            guage.register(registry);
            PushGateway pg = new PushGateway(url);
            Map<String, String> groupingKey = new HashMap<String, String>();
            groupingKey.put("instance", "my_instance");
            pg.pushAdd(registry, "my_job", groupingKey);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}