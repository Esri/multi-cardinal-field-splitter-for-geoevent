package com.esri.geoevent.processor.multicardinalfieldsplitter;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.BundleContext;

import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;

@ExtendWith(MockitoExtension.class)
class MulticardinalFieldSplitterServiceTest
{
  @Mock
  private Messaging mockMessaging;
  @Mock
  private GeoEventCreator mockGeoEventCreator;
  @Mock
  private BundleContext mockBundleContext;

  private MulticardinalFieldSplitterService service;

  @BeforeEach
  void setUp()
  {
    service = new MulticardinalFieldSplitterService();
  }

  @Test
  void constructorCreatesDefinition()
  {
    assertNotNull(service.getGeoEventProcessorDefinition(),
        "Service should have a definition after construction");
    assertInstanceOf(MulticardinalFieldSplitterDefinition.class,
        service.getGeoEventProcessorDefinition());
  }

  @Test
  void createReturnsProcessorInstance() throws Exception
  {
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    service.setMessaging(mockMessaging);
    service.setBundleContext(mockBundleContext);

    GeoEventProcessor processor = service.create();

    assertNotNull(processor, "create() should return a processor");
    assertInstanceOf(MulticardinalFieldSplitter.class, processor);
  }

  @Test
  void createSetsMessagingOnProcessor() throws Exception
  {
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    service.setMessaging(mockMessaging);
    service.setBundleContext(mockBundleContext);

    service.create();

    // createGeoEventCreator is called once inside the processor's setMessaging
    verify(mockMessaging).createGeoEventCreator();
  }

  @Test
  void definitionHasExpectedPropertyDefinitions()
  {
    Map<String, com.esri.ges.core.property.PropertyDefinition> propDefs = service.getGeoEventProcessorDefinition().getPropertyDefinitions();
    assertTrue(propDefs.containsKey("fieldToSplit"));
    assertTrue(propDefs.containsKey("newGeoEventDefinitionName"));
  }
}
