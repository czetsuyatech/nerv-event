package com.czetsuyatech.nerv.event.spring.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.consumer.EventHandlerInterceptor;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.spring.serialization.JacksonEventDeserializer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.Ordered;
import tools.jackson.databind.ObjectMapper;

class ConsumerAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(NervEventAutoConfiguration.class));

  @Test
  void acceptsNoEventHandlerBeans() {
    contextRunner.run(context -> {
      assertThat(context).hasSingleBean(EventHandlerRegistry.class);
      assertThat(context).doesNotHaveBean(ConsumerDispatcher.class);
    });
  }

  @Test
  void registersEventHandlerBeansInTheGenericRegistry() {
    EventHandler<String> handler = new StringHandler();

    contextRunner
        .withBean(
            EventHandler.class,
            () -> handler
        )
        .run(
            context -> assertThat(
                context.getBean(EventHandlerRegistry.class)
                    .handlerFor("order.created")
            ).isSameAs(handler)
        );
  }

  @Test
  void createsTheJsonDeserializerAndDispatcherWhenAnObjectMapperExists() {
    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .run(context -> {
          assertThat(context.getBean(EventDeserializer.class)).isInstanceOf(JacksonEventDeserializer.class);
          assertThat(context).hasSingleBean(ConsumerDispatcher.class);
        });
  }

  @Test
  void createsTheJsonDeserializerAndDispatcherWithBootsJacksonAutoConfiguration() {
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                NervEventAutoConfiguration.class,
                JacksonAutoConfiguration.class
            )
        )
        .run(context -> {
          assertThat(context).hasSingleBean(EventDeserializer.class);
          assertThat(context).hasSingleBean(EventSerializer.class);
          assertThat(context).hasSingleBean(ConsumerDispatcher.class);
        });
  }

  @Test
  void customRegistryAndDeserializerOverrideTheDefaults() {
    EventHandlerRegistry registry = new EventHandlerRegistry(List.of(new StringHandler()));
    EventDeserializer deserializer = new EventDeserializer() {
      @Override
      public <T> T deserialize(
          SerializedPayload payload,
          Class<T> payloadType
      ) {
        return payloadType.cast("custom");
      }
    };

    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .withBean(
            EventHandlerRegistry.class,
            () -> registry
        )
        .withBean(
            EventDeserializer.class,
            () -> deserializer
        )
        .run(context -> {
          assertThat(context.getBean(EventHandlerRegistry.class)).isSameAs(registry);
          assertThat(context.getBean(EventDeserializer.class)).isSameAs(deserializer);
          assertThat(context).hasSingleBean(ConsumerDispatcher.class);
        });
  }

  @Test
  void ordersInterceptorBeansUsingSpringOrdering() {
    List<String> calls = new ArrayList<>();
    EventHandler<String> handler = new StringHandler() {
      @Override
      public void handle(EventMessage<String> event) {
        calls.add("handler");
      }
    };

    contextRunner
        .withBean(
            ObjectMapper.class,
            ObjectMapper::new
        )
        .withBean(
            EventHandler.class,
            () -> handler
        )
        .withBean(
            "laterInterceptor",
            EventHandlerInterceptor.class,
            () -> new RecordingInterceptor(
                "later",
                20,
                calls
            )
        )
        .withBean(
            "earlierInterceptor",
            EventHandlerInterceptor.class,
            () -> new RecordingInterceptor(
                "earlier",
                10,
                calls
            )
        )
        .run(context -> {
          context.getBean(ConsumerDispatcher.class)
              .dispatch(
                  new ConsumerMessage(
                      new EventId("event-1"),
                      "order.created",
                      Instant.parse("2026-08-15T00:00:00Z"),
                      "orders",
                      null,
                      new SerializedPayload(
                          "\"payload\"",
                          "application/json"
                      )
                  )
              );

          assertThat(calls).containsExactly(
              "earlier-before",
              "later-before",
              "handler",
              "later-after",
              "earlier-after"
          );
        });
  }

  private static class StringHandler implements EventHandler<String> {
    @Override
    public String eventType() {
      return "order.created";
    }

    @Override
    public Class<String> payloadType() {
      return String.class;
    }

    @Override
    public void handle(EventMessage<String> event) {
    }
  }

  private static final class RecordingInterceptor implements EventHandlerInterceptor, Ordered {
    private final String name;
    private final int order;
    private final List<String> calls;

    private RecordingInterceptor(
        String name,
        int order,
        List<String> calls
    )
    {
      this.name = name;
      this.order = order;
      this.calls = calls;
    }

    @Override
    public int getOrder() {
      return order;
    }

    @Override
    public void intercept(
        EventMessage<?> event,
        com.czetsuyatech.nerv.event.consumer.EventHandlerChain chain
    ) {
      calls.add(name + "-before");
      try {
        chain.proceed();
      } finally {
        calls.add(name + "-after");
      }
    }
  }
}
