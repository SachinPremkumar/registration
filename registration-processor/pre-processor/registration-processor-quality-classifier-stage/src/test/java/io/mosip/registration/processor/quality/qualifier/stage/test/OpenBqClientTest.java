package io.mosip.registration.processor.quality.qualifier.stage.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import io.mosip.registration.processor.quality.classifier.openbq.dto.OpenBqTaskRequestDto;
import io.mosip.registration.processor.quality.classifier.openbq.service.OpenBqClient;

@RunWith(MockitoJUnitRunner.class)
public class OpenBqClientTest {

	private OpenBqClient openBqClient;

	@Mock
	private RestTemplate restTemplate;

	private ExecutorService testExecutor;

	private static final String TEST_URL = "http://localhost:8080/api/v1/tasks";
	private static final String TEST_REG_ID = "10002100010001020220928084430";

	@Before
	public void setUp() {
		openBqClient = new OpenBqClient();
		ReflectionTestUtils.setField(openBqClient, "openBqUrl", TEST_URL);
		ReflectionTestUtils.setField(openBqClient, "restTemplate", restTemplate);

		// Use single-thread executor for deterministic test execution
		testExecutor = Executors.newSingleThreadExecutor();
		ReflectionTestUtils.setField(openBqClient, "executorService", testExecutor);
	}

	@After
	public void tearDown() {
		openBqClient.shutdown();
		testExecutor.shutdownNow();
	}

	@Test
	public void testDispatchWhenDisabled() {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", false);

		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, "realtime");

		verify(restTemplate, never()).exchange(any(), any(), any(), eq(String.class));
	}

	@Test
	public void testDispatchWhenRegIdNullOrEmpty() {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", true);

		openBqClient.dispatchRegistrationIdAsync(null, "realtime");
		openBqClient.dispatchRegistrationIdAsync("   ", "realtime");

		verify(restTemplate, never()).exchange(any(), any(), any(), eq(String.class));
	}

	@Test
	public void testDispatchWhenSourceNullOrEmpty() {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", true);

		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, null);
		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, "   ");

		verify(restTemplate, never()).exchange(any(), any(), any(), eq(String.class));
	}

	@Test
	public void testDispatchExplicitRealtime() throws Exception {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", true);

		when(restTemplate.exchange(eq(TEST_URL), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>("{\"batch_id\":\"batch-123\"}", HttpStatus.CREATED));

		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, "realtime");

		// Wait for async task completion
		testExecutor.shutdown();
		assertTrue(testExecutor.awaitTermination(3, TimeUnit.SECONDS));

		ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
		verify(restTemplate).exchange(eq(TEST_URL), eq(HttpMethod.POST), entityCaptor.capture(), eq(String.class));

		HttpEntity capturedEntity = entityCaptor.getValue();
		assertNotNull(capturedEntity);
		assertTrue(capturedEntity.getBody() instanceof OpenBqTaskRequestDto);

		OpenBqTaskRequestDto requestDto = (OpenBqTaskRequestDto) capturedEntity.getBody();
		assertEquals("mosip", requestDto.getDataType());
		assertEquals("all", requestDto.getModality());
		assertEquals("realtime", requestDto.getSource());
		assertEquals(1, requestDto.getRegIds().size());
		assertEquals(TEST_REG_ID, requestDto.getRegIds().get(0));
	}

	@Test
	public void testDispatchExplicitExisting() throws Exception {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", true);

		when(restTemplate.exchange(eq(TEST_URL), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class)))
				.thenReturn(new ResponseEntity<>("{\"batch_id\":\"batch-123\"}", HttpStatus.CREATED));

		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, "existing");

		// Wait for async task completion
		testExecutor.shutdown();
		assertTrue(testExecutor.awaitTermination(3, TimeUnit.SECONDS));

		ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
		verify(restTemplate).exchange(eq(TEST_URL), eq(HttpMethod.POST), entityCaptor.capture(), eq(String.class));

		HttpEntity capturedEntity = entityCaptor.getValue();
		assertNotNull(capturedEntity);
		assertTrue(capturedEntity.getBody() instanceof OpenBqTaskRequestDto);

		OpenBqTaskRequestDto requestDto = (OpenBqTaskRequestDto) capturedEntity.getBody();
		assertEquals("mosip", requestDto.getDataType());
		assertEquals("all", requestDto.getModality());
		assertEquals("existing", requestDto.getSource());
		assertEquals(1, requestDto.getRegIds().size());
		assertEquals(TEST_REG_ID, requestDto.getRegIds().get(0));
	}

	@Test
	public void testDispatchHttpErrorSuppressed() throws Exception {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", true);

		when(restTemplate.exchange(eq(TEST_URL), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class)))
				.thenThrow(new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR, "OpenBQ Hub Error"));

		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, "realtime");

		testExecutor.shutdown();
		assertTrue(testExecutor.awaitTermination(3, TimeUnit.SECONDS));

		verify(restTemplate).exchange(eq(TEST_URL), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class));
	}

	@Test
	public void testDispatchConnectionTimeoutSuppressed() throws Exception {
		ReflectionTestUtils.setField(openBqClient, "openBqEnabled", true);

		when(restTemplate.exchange(eq(TEST_URL), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class)))
				.thenThrow(new ResourceAccessException("Connection timed out"));

		openBqClient.dispatchRegistrationIdAsync(TEST_REG_ID, "realtime");

		testExecutor.shutdown();
		assertTrue(testExecutor.awaitTermination(3, TimeUnit.SECONDS));

		verify(restTemplate).exchange(eq(TEST_URL), eq(HttpMethod.POST), any(HttpEntity.class), eq(String.class));
	}

	@Test
	public void testDefaultInit() {
		OpenBqClient client = new OpenBqClient();
		client.init();
		assertNotNull(ReflectionTestUtils.getField(client, "restTemplate"));
		assertFalse((Boolean) ReflectionTestUtils.getField(client, "openBqEnabled"));
		client.shutdown();
	}
}
