package com.jobdock.worker;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** Maps job type to handler. Exactly one handler per type. */
@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> byType;

    public HandlerRegistry(List<JobHandler> handlers) {
        this.byType = handlers.stream().collect(Collectors.toUnmodifiableMap(JobHandler::type, Function.identity(),
                (a, b) -> {
                    throw new IllegalStateException("Duplicate JobHandler for type '" + a.type() + "': "
                            + a.getClass().getName() + " and " + b.getClass().getName());
                }));
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(byType.get(type));
    }

    /** The job types this process can execute; workers only claim these. */
    public Set<String> types() {
        return byType.keySet();
    }
}
