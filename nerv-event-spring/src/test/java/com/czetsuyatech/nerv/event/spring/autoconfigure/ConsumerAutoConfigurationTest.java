package com.czetsuyatech.nerv.event.spring.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.spring.serialization.JacksonEventDeserializer;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
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

  private static final class StringHandler implements EventHandler<String> {
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
}
