package com.jobdock.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jobdock.job.EnqueueResult;
import com.jobdock.job.IdempotencyConflictException;
import com.jobdock.job.Job;
import com.jobdock.job.JobService;
import com.jobdock.job.JobStatus;
import com.jobdock.job.NewJob;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@WebMvcTest(JobController.class)
class JobControllerTest {

    @Autowired
    MockMvc mvc;

    @Autowired
    ObjectMapper om;

    @MockitoBean
    JobService service;

    @Test
    void createdJobReturns201WithLocation() throws Exception {
        Job job = job();
        when(service.enqueue(any())).thenReturn(new EnqueueResult.Created(job));

        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{\"to\":\"x\"}}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/jobs/" + job.id()))
                .andExpect(jsonPath("$.id").value(job.id().toString()))
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void replayReturns200WithReplayedHeader() throws Exception {
        when(service.enqueue(any())).thenReturn(new EnqueueResult.Replayed(job()));

        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{}}").header("Idempotency-Key", "k1"))
                .andExpect(status().isOk())
                .andExpect(header().string("Idempotent-Replayed", "true"));
    }

    @Test
    void keyConflictReturns409() throws Exception {
        when(service.enqueue(any())).thenThrow(new IdempotencyConflictException("k1"));

        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{}}").header("Idempotency-Key", "k1"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    void missingTypeReturns400() throws Exception {
        mvc.perform(postJob("{\"payload\":{}}")).andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void tooLongTypeReturns400() throws Exception {
        mvc.perform(postJob("{\"type\":\"" + "x".repeat(101) + "\",\"payload\":{}}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void missingOrNonObjectPayloadReturns400() throws Exception {
        mvc.perform(postJob("{\"type\":\"email\"}")).andExpect(status().isBadRequest());
        mvc.perform(postJob("{\"type\":\"email\",\"payload\":null}")).andExpect(status().isBadRequest());
        mvc.perform(postJob("{\"type\":\"email\",\"payload\":[1,2]}")).andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void maxAttemptsOutOfRangeReturns400() throws Exception {
        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{},\"maxAttempts\":0}"))
                .andExpect(status().isBadRequest());
        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{},\"maxAttempts\":101}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void maxAttemptsIsPassedThroughOrDefaulted() throws Exception {
        when(service.enqueue(any())).thenReturn(new EnqueueResult.Created(job()));

        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{},\"maxAttempts\":3}")).andExpect(status().isCreated());
        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{}}")).andExpect(status().isCreated());

        ArgumentCaptor<NewJob> captor = ArgumentCaptor.forClass(NewJob.class);
        verify(service, times(2)).enqueue(captor.capture());
        assertThat(captor.getAllValues()).extracting(NewJob::maxAttempts).containsExactly(3, NewJob.DEFAULT_MAX_ATTEMPTS);
    }

    @Test
    void malformedJsonReturns400() throws Exception {
        mvc.perform(postJob("{not json")).andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void tooLongIdempotencyKeyReturns400() throws Exception {
        mvc.perform(postJob("{\"type\":\"email\",\"payload\":{}}").header("Idempotency-Key", "k".repeat(256)))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    void getUnknownJobReturns404() throws Exception {
        UUID id = UUID.randomUUID();
        when(service.find(id)).thenReturn(Optional.empty());

        mvc.perform(get("/jobs/" + id)).andExpect(status().isNotFound());
    }

    @Test
    void getWithInvalidIdReturns400() throws Exception {
        mvc.perform(get("/jobs/not-a-uuid")).andExpect(status().isBadRequest());
    }

    private static MockHttpServletRequestBuilder postJob(String body) {
        return post("/jobs").contentType(MediaType.APPLICATION_JSON).content(body);
    }

    private Job job() {
        Instant now = Instant.now();
        return new Job(UUID.randomUUID(), "email", om.createObjectNode(), JobStatus.QUEUED, 0, 5, null, now, null,
                null, now, now, null, null);
    }
}
