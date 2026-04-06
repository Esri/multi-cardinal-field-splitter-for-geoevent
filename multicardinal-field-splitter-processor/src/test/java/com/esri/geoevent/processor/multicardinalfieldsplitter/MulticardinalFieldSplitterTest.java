package com.esri.geoevent.processor.multicardinalfieldsplitter;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldGroup;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.property.Property;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;

@ExtendWith(MockitoExtension.class)
class MulticardinalFieldSplitterTest
{
  @Mock
  private BundleContext mockBundleContext;
  @Mock
  private GeoEventDefinitionManager mockDefinitionManager;
  @Mock
  private Messaging mockMessaging;
  @Mock
  private GeoEventCreator mockGeoEventCreator;
  @Mock
  private GeoEventProducer mockGeoEventProducer;

  private MulticardinalFieldSplitterDefinition definition;
  private MulticardinalFieldSplitter processor;

  @BeforeEach
  void setUp() throws Exception
  {
    definition = new MulticardinalFieldSplitterDefinition();
    definition.setBundleContext(mockBundleContext);
    processor = new MulticardinalFieldSplitter(definition);

    // Wire up properties so afterPropertiesSet works
    PropertyDefinition pdField = new PropertyDefinition("fieldToSplit", PropertyType.String, "MyField", "label", "desc", true, false);
    PropertyDefinition pdDefName = new PropertyDefinition("newGeoEventDefinitionName", PropertyType.String, "FieldgroupSplitter", "label", "desc", true, false);
    processor.setProperty(new Property(pdField, "TestGroupField"));
    processor.setProperty(new Property(pdDefName, "TestOutputDef"));
  }

  // ── Construction ──

  @Test
  void constructorSucceeds()
  {
    assertNotNull(processor);
  }

  @Test
  void constructorWithNullBundleContextThrows()
  {
    MulticardinalFieldSplitterDefinition defNoBc = new MulticardinalFieldSplitterDefinition();
    // BundleContext is null → ServiceTracker constructor will fail
    assertThrows(Exception.class, () -> new MulticardinalFieldSplitter(defNoBc));
  }

  // ── afterPropertiesSet ──

  @Test
  void afterPropertiesSetReadsFieldToSplit() throws Exception
  {
    processor.afterPropertiesSet();
    String fieldToSplit = getPrivateField(processor, "fieldToSplit");
    assertEquals("TestGroupField", fieldToSplit);
  }

  @Test
  void afterPropertiesSetReadsGeoEventDefinitionName() throws Exception
  {
    processor.afterPropertiesSet();
    String defName = getPrivateField(processor, "geoEventDefinitionName");
    assertEquals("TestOutputDef", defName);
  }

  @Test
  void afterPropertiesSetMutatorWhenDefNameEmpty() throws Exception
  {
    PropertyDefinition pdDefName = new PropertyDefinition("newGeoEventDefinitionName", PropertyType.String, "", "label", "desc", true, false);
    processor.setProperty(new Property(pdDefName, ""));
    processor.afterPropertiesSet();
    assertTrue(processor.isGeoEventMutator(), "Should be mutator when definition name is empty");
  }

  @Test
  void afterPropertiesSetNotMutatorWhenDefNamePresent() throws Exception
  {
    processor.afterPropertiesSet();
    assertFalse(processor.isGeoEventMutator(), "Should not be mutator when definition name is set");
  }

  // ── validate ──

  @Test
  void validateThrowsWhenFieldToSplitIsNull() throws Exception
  {
    setPrivateField(processor, "fieldToSplit", null);
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);
    setPrivateField(processor, "geoEventCreator", mockGeoEventCreator);
    ValidationException ex = assertThrows(ValidationException.class, () -> processor.validate());
    assertTrue(ex.getMessage().contains("Field to Split"));
  }

  @Test
  void validateThrowsWhenFieldToSplitIsEmpty() throws Exception
  {
    setPrivateField(processor, "fieldToSplit", "  ");
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);
    setPrivateField(processor, "geoEventCreator", mockGeoEventCreator);
    ValidationException ex = assertThrows(ValidationException.class, () -> processor.validate());
    assertTrue(ex.getMessage().contains("Field to Split"));
  }

  @Test
  void validateThrowsWhenGeoEventDefinitionManagerIsNull() throws Exception
  {
    setPrivateField(processor, "fieldToSplit", "someField");
    setPrivateField(processor, "geoEventDefinitionManager", null);
    setPrivateField(processor, "geoEventCreator", mockGeoEventCreator);
    ValidationException ex = assertThrows(ValidationException.class, () -> processor.validate());
    assertTrue(ex.getMessage().contains("Definition Manager"));
  }

  @Test
  void validateThrowsWhenGeoEventCreatorIsNull() throws Exception
  {
    setPrivateField(processor, "fieldToSplit", "someField");
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);
    setPrivateField(processor, "geoEventCreator", null);
    ValidationException ex = assertThrows(ValidationException.class, () -> processor.validate());
    assertTrue(ex.getMessage().contains("GeoEvent Creator"));
  }

  @Test
  void validateCollectsAllErrors() throws Exception
  {
    setPrivateField(processor, "fieldToSplit", null);
    setPrivateField(processor, "geoEventDefinitionManager", null);
    setPrivateField(processor, "geoEventCreator", null);
    ValidationException ex = assertThrows(ValidationException.class, () -> processor.validate());
    String msg = ex.getMessage();
    assertTrue(msg.contains("Field to Split"), "Should report field to split error");
    assertTrue(msg.contains("Definition Manager"), "Should report definition manager error");
    assertTrue(msg.contains("GeoEvent Creator"), "Should report geo event creator error");
  }

  @Test
  void validateSucceedsWhenAllRequirementsAreMet() throws Exception
  {
    setPrivateField(processor, "fieldToSplit", "someField");
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);
    setPrivateField(processor, "geoEventCreator", mockGeoEventCreator);
    assertDoesNotThrow(() -> processor.validate());
  }

  // ── process ──

  @Test
  void processReturnsNullForNullEvent() throws Exception
  {
    assertNull(processor.process(null));
  }

  @Test
  void processReturnsNullForNonNullEvent() throws Exception
  {
    GeoEvent mockEvent = mock(GeoEvent.class);
    assertNull(processor.process(mockEvent), "process() always returns null (sends asynchronously)");
  }

  // ── setMessaging ──

  @Test
  void setMessagingCreatesGeoEventCreator()
  {
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    processor.setMessaging(mockMessaging);
    verify(mockMessaging).createGeoEventCreator();
  }

  // ── setId / event destinations ──

  @Test
  void setIdCreatesGeoEventProducer()
  {
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any(EventDestination.class))).thenReturn(mockGeoEventProducer);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");

    ArgumentCaptor<EventDestination> captor = ArgumentCaptor.forClass(EventDestination.class);
    verify(mockMessaging).createGeoEventProducer(captor.capture());
    assertEquals("test-id:event", captor.getValue().getName());
  }

  @Test
  void getEventDestinationsReturnsEmptyListWhenNoProducer()
  {
    List<EventDestination> destinations = processor.getEventDestinations();
    assertTrue(destinations.isEmpty());
  }

  @Test
  void getEventDestinationsReturnsProducerDestination()
  {
    EventDestination dest = new EventDestination("test-dest");
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any())).thenReturn(mockGeoEventProducer);
    when(mockGeoEventProducer.getEventDestination()).thenReturn(dest);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");

    List<EventDestination> destinations = processor.getEventDestinations();
    assertEquals(1, destinations.size());
    assertEquals("test-dest", destinations.get(0).getName());
  }

  @Test
  void getEventDestinationReturnsNullWhenNoProducer()
  {
    assertNull(processor.getEventDestination());
  }

  // ── send ──

  @Test
  void sendDelegatesToProducer() throws Exception
  {
    GeoEvent mockEvent = mock(GeoEvent.class);
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any())).thenReturn(mockGeoEventProducer);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");

    processor.send(mockEvent);
    verify(mockGeoEventProducer).send(mockEvent);
  }

  @Test
  void sendDoesNothingWithNullEvent() throws Exception
  {
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any())).thenReturn(mockGeoEventProducer);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");

    processor.send(null);
    verify(mockGeoEventProducer, never()).send(any());
  }

  // ── ServiceTrackerCustomizer ──

  @Test
  @SuppressWarnings("unchecked")
  void addingServiceSetsGeoEventDefinitionManager() throws Exception
  {
    ServiceReference<Object> mockRef = mock(ServiceReference.class);
    when(mockBundleContext.getService(mockRef)).thenReturn(mockDefinitionManager);

    Object result = processor.addingService(mockRef);

    assertEquals(mockDefinitionManager, result);
    GeoEventDefinitionManager stored = getPrivateField(processor, "geoEventDefinitionManager");
    assertEquals(mockDefinitionManager, stored);
  }

  @Test
  @SuppressWarnings("unchecked")
  void addingServiceIgnoresNonManagerService() throws Exception
  {
    ServiceReference<Object> mockRef = mock(ServiceReference.class);
    Object somethingElse = "not a manager";
    when(mockBundleContext.getService(mockRef)).thenReturn(somethingElse);

    Object result = processor.addingService(mockRef);

    assertEquals(somethingElse, result);
    GeoEventDefinitionManager stored = getPrivateField(processor, "geoEventDefinitionManager");
    assertNull(stored);
  }

  @Test
  @SuppressWarnings("unchecked")
  void removedServiceClearsGeoEventDefinitionManager() throws Exception
  {
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);

    ServiceReference<Object> mockRef = mock(ServiceReference.class);
    processor.removedService(mockRef, mockDefinitionManager);

    GeoEventDefinitionManager stored = getPrivateField(processor, "geoEventDefinitionManager");
    assertNull(stored, "Manager should be cleared after service removal");
  }

  @Test
  @SuppressWarnings("unchecked")
  void removedServiceDoesNotClearManagerForOtherServices() throws Exception
  {
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);

    ServiceReference<Object> mockRef = mock(ServiceReference.class);
    processor.removedService(mockRef, "something else");

    GeoEventDefinitionManager stored = getPrivateField(processor, "geoEventDefinitionManager");
    assertNotNull(stored, "Manager should not be cleared for unrelated service removal");
  }

  // ── disconnect / isConnected / getStatusDetails ──

  @Test
  void disconnectWithNoProducerDoesNotThrow()
  {
    assertDoesNotThrow(() -> processor.disconnect());
  }

  @Test
  void disconnectDelegatesToProducer()
  {
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any())).thenReturn(mockGeoEventProducer);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");

    processor.disconnect();
    verify(mockGeoEventProducer).disconnect();
  }

  @Test
  void isConnectedReturnsFalseWithNoProducer()
  {
    assertFalse(processor.isConnected());
  }

  @Test
  void getStatusDetailsReturnsEmptyWithNoProducer()
  {
    assertEquals("", processor.getStatusDetails());
  }

  // ── shutdown ──

  @Test
  void shutdownDoesNotThrow() throws Exception
  {
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);
    assertDoesNotThrow(() -> processor.shutdown());
  }

  // ── fieldCardinalSplit integration (via process + executor) ──

  @Test
  void processWithGroupFieldSplitsIntoMultipleEvents() throws Exception
  {
    // Set up processor state
    processor.afterPropertiesSet();
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any())).thenReturn(mockGeoEventProducer);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);

    // Build mock GeoEventDefinition with a Group field containing 2 children
    GeoEventDefinition mockEdIn = mock(GeoEventDefinition.class);
    when(mockEdIn.getGuid()).thenReturn("guid-in");

    FieldDefinition childField1 = mock(FieldDefinition.class);
    when(childField1.getName()).thenReturn("childA");
    when(childField1.clone()).thenReturn(childField1);
    FieldDefinition childField2 = mock(FieldDefinition.class);
    when(childField2.getName()).thenReturn("childB");
    when(childField2.clone()).thenReturn(childField2);

    FieldDefinition groupFieldDef = mock(FieldDefinition.class);
    when(groupFieldDef.getType()).thenReturn(FieldType.Group);
    when(groupFieldDef.getName()).thenReturn("TestGroupField");
    when(groupFieldDef.getChildren()).thenReturn(Arrays.asList(childField1, childField2));

    when(mockEdIn.getFieldDefinition("TestGroupField")).thenReturn(groupFieldDef);
    when(mockEdIn.getIndexOf("TestGroupField")).thenReturn(1);
    when(mockEdIn.getFieldDefinitions()).thenReturn(Collections.emptyList());

    // Mock reduce/augment chain for lookup()
    GeoEventDefinition mockEdOut = mock(GeoEventDefinition.class);
    when(mockEdOut.getGuid()).thenReturn("guid-out");
    GeoEventDefinition mockReduced = mock(GeoEventDefinition.class);
    when(mockEdIn.reduce(anyList())).thenReturn(mockReduced);
    GeoEventDefinition mockAugmented1 = mock(GeoEventDefinition.class);
    when(mockReduced.augment(anyList())).thenReturn(mockAugmented1);
    when(mockAugmented1.augment(anyList())).thenReturn(mockEdOut);

    // Build mock source GeoEvent with 2 field groups
    GeoEvent mockSourceEvent = mock(GeoEvent.class);
    when(mockSourceEvent.getGeoEventDefinition()).thenReturn(mockEdIn);
    when(mockSourceEvent.getAllFields()).thenReturn(new Object[] { "val0", null, "val2" });
    when(mockSourceEvent.getProperties()).thenReturn(Collections.emptySet());

    FieldGroup fg1 = mock(FieldGroup.class);
    when(fg1.getField(0)).thenReturn("child1Val1");
    when(fg1.getField(1)).thenReturn("child1Val2");
    FieldGroup fg2 = mock(FieldGroup.class);
    when(fg2.getField(0)).thenReturn("child2Val1");
    when(fg2.getField(1)).thenReturn("child2Val2");
    when(mockSourceEvent.getFieldGroups("TestGroupField")).thenReturn(Arrays.asList(fg1, fg2));

    // Mock GeoEventCreator to return mock output events
    GeoEvent mockOutEvent1 = mock(GeoEvent.class);
    GeoEvent mockOutEvent2 = mock(GeoEvent.class);
    when(mockGeoEventCreator.create(eq("guid-out"), any(Object[].class)))
        .thenReturn(mockOutEvent1, mockOutEvent2);

    // Execute
    processor.process(mockSourceEvent);
    Thread.sleep(500);

    // Verify 2 events were sent (one per field group)
    verify(mockGeoEventProducer, timeout(2000).times(2)).send(any(GeoEvent.class));
  }

  @Test
  void processWithNonGroupFieldSplitsList() throws Exception
  {
    // Set up processor state
    processor.afterPropertiesSet();
    when(mockMessaging.createGeoEventCreator()).thenReturn(mockGeoEventCreator);
    when(mockMessaging.createGeoEventProducer(any())).thenReturn(mockGeoEventProducer);
    processor.setMessaging(mockMessaging);
    processor.setId("test-id");
    setPrivateField(processor, "geoEventDefinitionManager", mockDefinitionManager);

    // Build mock definition with a non-Group multicardinal field
    GeoEventDefinition mockEdIn = mock(GeoEventDefinition.class);
    when(mockEdIn.getGuid()).thenReturn("guid-in-2");

    FieldDefinition listFieldDef = mock(FieldDefinition.class);
    when(listFieldDef.getType()).thenReturn(FieldType.String);
    when(listFieldDef.getName()).thenReturn("TestGroupField");
    when(listFieldDef.getChildren()).thenReturn(Collections.emptyList());
    when(listFieldDef.clone()).thenReturn(listFieldDef);

    when(mockEdIn.getFieldDefinition("TestGroupField")).thenReturn(listFieldDef);
    when(mockEdIn.getIndexOf("TestGroupField")).thenReturn(0);
    when(mockEdIn.getFieldDefinitions()).thenReturn(Collections.emptyList());

    // Mock reduce/augment chain for lookup()
    GeoEventDefinition mockEdOut = mock(GeoEventDefinition.class);
    when(mockEdOut.getGuid()).thenReturn("guid-out-2");
    GeoEventDefinition mockReduced = mock(GeoEventDefinition.class);
    when(mockEdIn.reduce(anyList())).thenReturn(mockReduced);
    GeoEventDefinition mockAugmented = mock(GeoEventDefinition.class);
    when(mockReduced.augment(anyList())).thenReturn(mockAugmented);
    when(mockAugmented.augment(anyList())).thenReturn(mockEdOut);

    // Build source event with a list of 3 values
    GeoEvent mockSourceEvent = mock(GeoEvent.class);
    when(mockSourceEvent.getGeoEventDefinition()).thenReturn(mockEdIn);
    List<Object> fieldValues = Arrays.asList("alpha", "beta", "gamma");
    when(mockSourceEvent.getField("TestGroupField")).thenReturn(fieldValues);
    when(mockSourceEvent.getAllFields()).thenReturn(new Object[] { fieldValues, "other" });
    when(mockSourceEvent.getProperties()).thenReturn(Collections.emptySet());

    // Mock creator
    GeoEvent mockOut = mock(GeoEvent.class);
    when(mockGeoEventCreator.create(eq("guid-out-2"), any(Object[].class))).thenReturn(mockOut);

    // Execute
    processor.process(mockSourceEvent);

    // Verify 3 events sent (one per list item)
    verify(mockGeoEventProducer, timeout(2000).times(3)).send(any(GeoEvent.class));
  }

  // ── Helpers ──

  @SuppressWarnings("unchecked")
  private static <T> T getPrivateField(Object target, String fieldName) throws Exception
  {
    Class<?> clazz = target.getClass();
    while (clazz != null)
    {
      try
      {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
      }
      catch (NoSuchFieldException e)
      {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static void setPrivateField(Object target, String fieldName, Object value) throws Exception
  {
    Class<?> clazz = target.getClass();
    while (clazz != null)
    {
      try
      {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      }
      catch (NoSuchFieldException e)
      {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
