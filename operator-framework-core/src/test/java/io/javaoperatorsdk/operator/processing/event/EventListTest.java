package io.javaoperatorsdk.operator.processing.event;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.internal.TimerEvent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class EventListTest {

  @Test
  public void returnsLatestOfEventType() {
    TimerEvent event2 = new TimerEvent("1", null);
    EventList eventList =
        new EventList(
            Arrays.asList(mock(Event.class), new TimerEvent("2", null), event2, mock(Event.class)));

    assertThat(eventList.getLatestOfType(TimerEvent.class).get()).isEqualTo(event2);
  }
}
