package com.jobdock.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class HandlerRegistryTest {

    @Test
    void findsHandlerByType() {
        JobHandler email = handler("email");
        HandlerRegistry registry = new HandlerRegistry(List.of(email, handler("sms")));

        assertThat(registry.find("email")).containsSame(email);
        assertThat(registry.find("fax")).isEmpty();
        assertThat(registry.types()).containsExactlyInAnyOrder("email", "sms");
    }

    @Test
    void emptyRegistryHasNoTypes() {
        assertThat(new HandlerRegistry(List.of()).types()).isEmpty();
    }

    @Test
    void duplicateTypeIsRejected() {
        assertThatThrownBy(() -> new HandlerRegistry(List.of(handler("email"), handler("email"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("email");
    }

    static JobHandler handler(String type) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(com.jobdock.job.Job job) {}
        };
    }
}
