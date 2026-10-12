package io.mosip.registration.processor.quality.classifier.openbq.service;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import io.mosip.registration.processor.quality.classifier.openbq.dto.OpenBqTaskRequestDto;

/**
 * Service to dispatch Registration IDs (RIDs) to the OpenBQ middleware
 * asynchronously and in a non-blocking, fault-tolerant manner.
 */
@Component
public class OpenBqClient {

	private static final Logger regProcLogger = RegProcessorLogger.getLogger(OpenBqClient.class);

	@Value("${mosip.regproc.quality.classifier.openbq.enabled:false}")
	private Boolean openBqEnabled = Boolean.FALSE;

	@Value("${mosip.regproc.quality.classifier.openbq.url:http://mosip-openbq-middleware:8081/tasks}")
	private String openBqUrl = "http://mosip-openbq-middleware:8081/tasks";

	@Value("${mosip.regproc.quality.classifier.openbq.timeout:5000}")
	private Integer timeout = 5000;

	private RestTemplate restTemplate;

	private ExecutorService executorService;

	@PostConstruct
	public void init() {
		if (this.restTemplate == null) {
			SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
			int timeoutMs = (timeout != null && timeout > 0) ? timeout : 5000;
			factory.setConnectTimeout(timeoutMs);
			factory.setReadTimeout(timeoutMs);
			this.restTemplate = new RestTemplate(factory);
		}

		if (this.executorService == null) {
			this.executorService = Executors.newFixedThreadPool(
					Math.max(2, Runtime.getRuntime().availableProcessors()),
					new ThreadFactory() {
						private final AtomicInteger threadNumber = new AtomicInteger(1);

						@Override
						public Thread newThread(Runnable r) {
							Thread t = new Thread(r, "openbq-dispatcher-" + threadNumber.getAndIncrement());
							t.setDaemon(true);
							return t;
						}
					});
		}
	}

	/**
	 * Dispatches the registration ID to OpenBQ Hub asynchronously with an explicit source.
	 * Live incoming registrations must pass "realtime".
	 * Batch/reprocessing registrations from existing DB must pass "existing".
	 *
	 * Any errors encountered (timeouts, connection failures, HTTP errors) are
	 * caught, logged as warnings, and suppressed to guarantee zero impact on the MOSIP pipeline.
	 *
	 * @param regId  the registration ID
	 * @param source the source classification ("realtime" or "existing")
	 */
	public void dispatchRegistrationIdAsync(String regId, String source) {
		if (!Boolean.TRUE.equals(openBqEnabled)) {
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(),
					LoggerFileConstant.REGISTRATIONID.toString(), regId,
					"OpenBQ integration is disabled. Skipping dispatch.");
			return;
		}

		if (regId == null || regId.trim().isEmpty()) {
			regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(),
					LoggerFileConstant.REGISTRATIONID.toString(), regId,
					"Registration ID is null or empty. Skipping OpenBQ dispatch.");
			return;
		}

		if (source == null || source.trim().isEmpty()) {
			regProcLogger.warn(LoggerFileConstant.SESSIONID.toString(),
					LoggerFileConstant.REGISTRATIONID.toString(), regId,
					"OpenBQ dispatch source is null or empty. Skipping dispatch because source is mandatory.");
			return;
		}

		if (executorService == null || executorService.isShutdown()) {
			regProcLogger.warn(LoggerFileConstant.SESSIONID.toString(),
					LoggerFileConstant.REGISTRATIONID.toString(), regId,
					"OpenBQ executor service is not active. Skipping dispatch.");
			return;
		}

		final String dispatchSource = source.trim();

		executorService.submit(() -> {
			try {
				OpenBqTaskRequestDto requestDto = new OpenBqTaskRequestDto(Collections.singletonList(regId), dispatchSource);
				requestDto.setModality("all");

				HttpHeaders headers = new HttpHeaders();
				headers.setContentType(MediaType.APPLICATION_JSON);
				headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
				HttpEntity<OpenBqTaskRequestDto> requestEntity = new HttpEntity<>(requestDto, headers);

				regProcLogger.info(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), regId,
						"OpenBqClient::dispatchRegistrationIdAsync - Dispatching RID to OpenBQ at " + openBqUrl
								+ " with source: " + dispatchSource);

				ResponseEntity<String> response = restTemplate.exchange(openBqUrl, HttpMethod.POST, requestEntity,
						String.class);

				regProcLogger.info(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), regId,
						"OpenBqClient::dispatchRegistrationIdAsync - OpenBQ response status: "
								+ response.getStatusCode());
			} catch (Exception e) {
				regProcLogger.warn(LoggerFileConstant.SESSIONID.toString(),
						LoggerFileConstant.REGISTRATIONID.toString(), regId,
						"OpenBqClient::dispatchRegistrationIdAsync - Failed to dispatch to OpenBQ (non-blocking, suppressed): "
								+ e.getMessage());
			}
		});
	}

	@PreDestroy
	public void shutdown() {
		if (executorService != null && !executorService.isShutdown()) {
			executorService.shutdown();
			try {
				if (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
					executorService.shutdownNow();
				}
			} catch (InterruptedException e) {
				executorService.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}
}
