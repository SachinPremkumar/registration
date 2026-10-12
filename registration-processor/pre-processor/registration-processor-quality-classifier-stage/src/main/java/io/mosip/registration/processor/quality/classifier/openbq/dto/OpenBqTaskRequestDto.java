package io.mosip.registration.processor.quality.classifier.openbq.dto;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request DTO for submitting biometric quality assessment tasks to OpenBQ Hub.
 * Matches OpenBQ Hub's task ingestion schema (POST /api/v1/tasks).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenBqTaskRequestDto implements Serializable {

	private static final long serialVersionUID = 1L;

	@JsonProperty("data_type")
	private String dataType = "mosip";

	@JsonProperty("reg_ids")
	private List<String> regIds;

	@JsonProperty("modality")
	private String modality = "all";

	@JsonProperty("source")
	private String source;

	public OpenBqTaskRequestDto(List<String> regIds, String source) {
		this.dataType = "mosip";
		this.regIds = regIds;
		this.modality = "all";
		this.source = source;
	}
}
