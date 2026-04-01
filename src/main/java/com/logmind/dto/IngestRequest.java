package main.java.com.logmind.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.logmind.entity.LogLevel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class IngestRequest {

    @NotBlank(message = "service name is required")
    @Size(max = 100, message = "service name must be 100 characters or fewer")
    @Pattern(regexp = "^[a-zA-Z0-9_-]+$", message = "service name may only contain letters, numbers, hyphens and underscores")
    private String service;

    @NotNull(message = "log level is required")
    private LogLevel level;

    @NotBlank(message = "message is required")
    @Size(max = 10000, message = "message must be 10,000 characters or fewer")
    private String message;

    // Optional — any extra key/value pairs the caller wants to attach
    private Map<String, Object> metadata;

    // Optional — defaults to now() in the service layer if absent
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    private LocalDateTime timestamp;
}