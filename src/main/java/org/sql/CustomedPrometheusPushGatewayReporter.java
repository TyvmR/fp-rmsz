/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sql;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.BasicAuthHttpConnectionFactory;
import io.prometheus.client.exporter.HttpConnectionFactory;
import io.prometheus.client.exporter.PushGateway;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.*;

import static org.sql.CustomedPrometheusPushGatewayReporterOptions.*;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Prometheus {@link PushGateway}.
 */
@PublicEvolving
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.prometheus.CustomedPrometheusPushGatewayReporterFactory")
public class CustomedPrometheusPushGatewayReporter extends CustomedAbstractPrometheusReporter implements Scheduled, CharacterFilter {

	private PushGateway pushGateway;
	private String jobName;
	private boolean deleteOnShutdown;
	private Map<String, String> groupingKey;
	/**
	 * used for limit report metricName to only needed ones.
	 * if this is Empty ,represent not to use.
	 */
	private static final Set<String> onlyReportNames=new HashSet<>(1<<5);

	@Override
	public void open(MetricConfig config) {
		super.open(config);
		ObjectMapper objectMapper=new ObjectMapper();
		try {
			log.info("metricName config{}", objectMapper.writeValueAsString(config));
		} catch (JsonProcessingException e) {
			log.error(e.getMessage(), e);
		}
		String host = config.getString(HOST.key(), HOST.defaultValue());
		int port = config.getInteger(PORT.key(), PORT.defaultValue());
		String configuredJobName = config.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
		boolean randomSuffix = config.getBoolean(RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());
		deleteOnShutdown = config.getBoolean(DELETE_ON_SHUTDOWN.key(), DELETE_ON_SHUTDOWN.defaultValue());
		groupingKey = parseGroupingKey(config.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));
		tryAddOnlyReportNames(config.getString(ONLY_REPORT_NAMES.key(),ONLY_REPORT_NAMES.defaultValue()));
		if (host == null || host.isEmpty() || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		if (randomSuffix) {
			this.jobName = configuredJobName + new AbstractID();
		} else {
			this.jobName = configuredJobName;
		}

		pushGateway = new PushGateway(host + ':' + port);

		if (!StringUtils.isNullOrWhitespaceOnly(config.getString(USERNAME.key(),USERNAME.defaultValue()))
		&& !StringUtils.isNullOrWhitespaceOnly(config.getString(PASSWORD.key(),PASSWORD.defaultValue()))){
			HttpConnectionFactory connectionFactory=new BasicAuthHttpConnectionFactory(config.getProperty(USERNAME.key()),config.getProperty(PASSWORD.key()));
			pushGateway.setConnectionFactory(connectionFactory);
		}
		log.info("Configured PrometheusPushGatewayReporter with {host:{}, port:{}, jobName:{}, randomJobNameSuffix:{}, deleteOnShutdown:{}, groupingKey:{}}",
			host, port, jobName, randomSuffix, deleteOnShutdown, groupingKey);
	}
	private boolean ifMetricNameHasConstraint(){
		return ! onlyReportNames.isEmpty();
	}
	private void tryAddOnlyReportNames(String string) {
		if (StringUtils.isNullOrWhitespaceOnly(string)){
			log.info("Only reported metricNames is empty");
			return;
		}
		String[] split = string.trim().split(",");
		log.info("Only reported metricNames:");
		for (String metricName:split){
			onlyReportNames.add(metricName);
			log.info("metricName:{}",metricName);
		}
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {

		String scopedName = CustomedAbstractPrometheusReporter.getScopedName(metricName, group);
		//如果有metricName限制列表则只报告这些,配置的Names实际是scopedName格式的，所以要先转成对应格式。
		if (ifMetricNameHasConstraint()){
			if(onlyReportNames.contains(scopedName)){
				super.notifyOfAddedMetric(metric, metricName, group);
			}
			//如果没有metricName限制列表全部报告。
		}else{
			super.notifyOfAddedMetric(metric, metricName, group);
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {

		String scopedName = CustomedAbstractPrometheusReporter.getScopedName(metricName, group);
		//如果有metricName限制列表则只删除这些,配置的Names实际是scopedName格式的，所以要先转成对应格式。
		if (ifMetricNameHasConstraint()){
			if(onlyReportNames.contains(scopedName)){
				super.notifyOfRemovedMetric(metric, metricName, group);
			}
			//如果没有metricName限制列表全部报告。
		}else{
			super.notifyOfRemovedMetric(metric, metricName, group);
		}
	}

	Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
		if (!groupingKeyConfig.isEmpty()) {
			Map<String, String> groupingKey = new HashMap<>();
			String[] kvs = groupingKeyConfig.split(";");
			for (String kv : kvs) {
				int idx = kv.indexOf("=");
				if (idx < 0) {
					log.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
					continue;
				}

				String labelKey = kv.substring(0, idx);
				String labelValue = kv.substring(idx + 1);
				if (StringUtils.isNullOrWhitespaceOnly(labelKey) || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
					log.warn("Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty", labelKey, labelValue);
					continue;
				}
				groupingKey.put(labelKey, labelValue);
			}

			return groupingKey;
		}
		return Collections.emptyMap();
	}

	@Override
	public void report() {
		try {
			pushGateway.push(CollectorRegistry.defaultRegistry, jobName, groupingKey);
		} catch (Exception e) {
			log.warn("Failed to push metrics to PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
		}
	}

	@Override
	public void close() {
		if (deleteOnShutdown && pushGateway != null) {
			try {
				pushGateway.delete(jobName, groupingKey);
			} catch (IOException e) {
				log.warn("Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.", jobName, groupingKey, e);
			}
		}
		super.close();
	}

	@Override
	public String filterCharacters(String s) {
		return s;
	}
}
