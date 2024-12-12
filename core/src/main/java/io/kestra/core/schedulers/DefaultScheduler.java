package io.kestra.core.schedulers;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.Flow;
import io.kestra.core.models.triggers.Trigger;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.FlowRepositoryInterface;
import io.kestra.core.services.ConditionService;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.core.utils.Await;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

@Slf4j
@Singleton
//TODO maybe move it to the MemoryRunner ?
public class DefaultScheduler extends AbstractScheduler {
    private final Map<String, Trigger> watchingTrigger = new ConcurrentHashMap<>();

    private final ConditionService conditionService;

    private final FlowRepositoryInterface flowRepository;

    private final ScheduledExecutorService scheduleExecutor = Executors.newSingleThreadScheduledExecutor();

    @Inject
    public DefaultScheduler(
        ApplicationContext applicationContext,
        FlowListenersInterface flowListeners,
        SchedulerExecutionStateInterface executionState,
        SchedulerTriggerStateInterface triggerState
    ) {
        super(applicationContext, flowListeners);
        this.triggerState = triggerState;
        this.executionState = executionState;

        this.conditionService = applicationContext.getBean(ConditionService.class);
        this.flowRepository = applicationContext.getBean(FlowRepositoryInterface.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
        QueueInterface<Execution> executionQueue = applicationContext.getBean(QueueInterface.class, Qualifiers.byName(QueueFactoryInterface.EXECUTION_NAMED));
        QueueInterface<Trigger> triggerQueue = applicationContext.getBean(QueueInterface.class, Qualifiers.byName(QueueFactoryInterface.TRIGGER_NAMED));

        super.run();

        ScheduledFuture<?> handle = scheduleExecutor.scheduleAtFixedRate(
            this::handle,
            0,
            1,
            TimeUnit.SECONDS
        );

        // look at exception on the main thread
        Thread thread = new Thread(
            () -> {
                Await.until(handle::isDone);

                try {
                    handle.get();
                } catch (CancellationException ignored) {

                } catch (ExecutionException | InterruptedException e) {
                    log.error("Scheduler fatal exception", e);
                    close();
                    applicationContext.close();
                }
            },
            "scheduler-listener"
        );
        thread.start();

        executionQueue.receive(either -> {
            if (either.isRight()) {
                log.error("Unable to deserialize and execution: {}", either.getRight().getMessage());
                return;
            }

            Execution execution = either.getLeft();
            if (execution.getTrigger() != null) {
                Trigger trigger = Await.until(()  -> watchingTrigger.get(execution.getId()), Duration.ofSeconds(5));
                var flow = flowRepository.findById(execution.getTenantId(), execution.getNamespace(), execution.getFlowId()).orElse(null);
                if (execution.isDeleted() || conditionService.isTerminatedWithListeners(flow, execution)) {
                    triggerState.update(trigger.resetExecution(execution.getState().getCurrent()));
                    watchingTrigger.remove(execution.getId());
                } else {
                    triggerState.update(Trigger.of(execution, trigger));
                }
            }
        });

        triggerQueue.receive(either -> {
            if (either.isRight()) {
                log.error("Unable to deserialize a trigger: {}", either.getRight().getMessage());
                return;
            }

            Trigger trigger = either.getLeft();
            if (trigger != null && trigger.getExecutionId() != null) {
                this.watchingTrigger.put(trigger.getExecutionId(), trigger);
            }
        });

        super.run();
    }

    @Override
    public void handleNext(List<Flow> flows, ZonedDateTime now, BiConsumer<List<Trigger>, ScheduleContextInterface> consumer) {
        List<Trigger> triggers =  triggerState.findAllForAllTenants().stream().filter(trigger -> trigger.getNextExecutionDate() == null || trigger.getNextExecutionDate().isBefore(now)).toList();
        DefaultScheduleContext schedulerContext = new DefaultScheduleContext();
        consumer.accept(triggers, schedulerContext);
    }

    @Override
    @PreDestroy
    public void close() {
        this.scheduleExecutor.shutdown();
    }
}
