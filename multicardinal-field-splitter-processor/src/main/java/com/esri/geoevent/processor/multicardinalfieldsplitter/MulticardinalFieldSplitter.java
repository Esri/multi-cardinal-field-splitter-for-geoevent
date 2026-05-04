/*
  Copyright 2019 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.​

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.processor.multicardinalfieldsplitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Observable;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.FieldCardinality;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldGroup;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManager;
import com.esri.ges.manager.geoeventdefinition.GeoEventDefinitionManagerException;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;

public class MulticardinalFieldSplitter extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable, ServiceTrackerCustomizer<Object, Object>
{
  private static final BundleLogger log              = BundleLoggerFactory.getLogger(MulticardinalFieldSplitter.class);
  private static final String       INDEX_FIELD_NAME = "childIndex";
  private static final int          SPLIT_THREAD_COUNT = Math.max(2, Runtime.getRuntime().availableProcessors());
  private static final int          SPLIT_QUEUE_CAPACITY = 1024;

  private final Map<String, String>       edMapper         = new ConcurrentHashMap<>();
  private ServiceTracker<?, ?>      geoEventDefinitionManagerTracker;
  private GeoEventDefinitionManager geoEventDefinitionManager;
  private Messaging                 messaging;
  private GeoEventCreator           geoEventCreator;
  private GeoEventProducer          geoEventProducer;
  private String                    fieldToSplit;

  private String                    geoEventDefinitionName;
  private ExecutorService           executor;

  protected MulticardinalFieldSplitter(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
    if (geoEventDefinitionManagerTracker == null)
      geoEventDefinitionManagerTracker = new ServiceTracker<>(definition.getBundleContext(), GeoEventDefinitionManager.class.getName(), this);
    geoEventDefinitionManagerTracker.open();
    log.trace("Event Splitter instantiated.");
  }

  @Override
  public void afterPropertiesSet()
  {
    if (log.isTraceEnabled())
    {
      getProperties().forEach(prop ->
        {
          log.trace(prop.toString());
        });
    }
    fieldToSplit = getProperty("fieldToSplit").getValueAsString();
    geoEventDefinitionName = getProperty("newGeoEventDefinitionName").getValueAsString();
    geoEventMutator = geoEventDefinitionName.isEmpty();
    log.trace("Field to split is {0} and new defintion name is {1} and isMutator = {2}", fieldToSplit, geoEventDefinitionName, geoEventMutator);
  }

  @Override
  public void setId(String id)
  {
    super.setId(id);
    geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
  }

  @Override
  public GeoEvent process(GeoEvent geoEvent) throws Exception
  {
    if(geoEvent != null){
      GeoEventSplitter splitter = new GeoEventSplitter(geoEvent);
      try
      {
        getExecutor().execute(splitter);
      }
      catch (RejectedExecutionException e)
      {
        log.warn("Field Cardinal Split dropped GeoEvent {0}. Split executor queue is full.", geoEvent.getGuid());
      }
    }
    return null;
  }

  private synchronized ExecutorService getExecutor()
  {
    if (executor == null || executor.isShutdown() || executor.isTerminated())
      executor = new ThreadPoolExecutor(SPLIT_THREAD_COUNT, SPLIT_THREAD_COUNT, 30L, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(SPLIT_QUEUE_CAPACITY));
    return executor;
  }

  @Override
  public List<EventDestination> getEventDestinations()
  {
    return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<>();
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    List<String> errors = new ArrayList<>();
    if (fieldToSplit == null || fieldToSplit.trim().isEmpty())
      errors.add("'Field to Split' is required.");
    if (geoEventDefinitionManager == null)
      errors.add("GeoEvent Definition Manager is not available.");
    if (geoEventCreator == null)
      errors.add("GeoEvent Creator is not available. Messaging may not have been initialized.");
    if (!errors.isEmpty())
    {
      StringBuilder sb = new StringBuilder();
      for (String message : errors)
        sb.append(message).append("\n");
      throw new ValidationException(this.getClass().getName() + " validation failed: " + sb.toString());
    }
  }

  @Override
  public EventDestination getEventDestination()
  {
    return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
  }

  @Override
  public void send(GeoEvent geoEvent) throws MessagingException
  {
    if (geoEventProducer != null && geoEvent != null)
    {
      geoEventProducer.send(geoEvent);
    }
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
    geoEventCreator = messaging.createGeoEventCreator();
  }

  private void fieldCardinalSplit(GeoEvent sourceGeoEvent) throws MessagingException
  {
    if (geoEventCreator != null)
    {
      try
      {
        GeoEventDefinition ed = sourceGeoEvent.getGeoEventDefinition();
        log.trace("Splitting incoming definition {0}", ed);
        FieldDefinition fdToSplit = ed.getFieldDefinition(fieldToSplit);
        // Missing or wrongly named split fields still fall through to the generic error log below.
        log.trace("Splitting field named {0} with definition {1}", fieldToSplit, fdToSplit);

        GeoEventDefinition edOut = lookup(sourceGeoEvent.getGeoEventDefinition(), fdToSplit);
        if (Thread.interrupted())
          return;
        if (fdToSplit.getType() == FieldType.Group)
        {
          int childId = 0;
          log.trace("Field definition to split is a group");
          List<FieldGroup> fieldgroups = null;
          FieldGroup fieldgroup = null;
          try
          {
            fieldgroups = sourceGeoEvent.getFieldGroups(fieldToSplit);
          }
          catch (Exception e)
          {
            log.trace("Field definition to split is not a multicardinal field group. Trying to get it as a single field group: ", e);
            fieldgroup = sourceGeoEvent.getFieldGroup(fieldToSplit);
          }

          if (fieldgroups == null && fieldgroup != null)
          {
            childId = sendFieldGroupMember(sourceGeoEvent, fdToSplit, edOut, childId, fieldgroup);
          }
          else if (fieldgroups == null || fieldgroups.isEmpty())
          {
            log.trace("Child field groups is null or size 0.");
            appendFieldValuesAndSend(sourceGeoEvent, edOut, null, getSplitFieldOutputFieldCount(fdToSplit), -1);
          }
          else
          {
          for (FieldGroup fg : fieldgroups)
          {
              childId = sendFieldGroupMember(sourceGeoEvent, fdToSplit, edOut, childId, fg);
            }
          }
        }
        else
        {
          log.debug("Field defintion to split is not a group: {0}", fdToSplit.getType());
          Object fieldValue = sourceGeoEvent.getField(fieldToSplit);
          if (fieldValue != null && !(fieldValue instanceof List<?>))
          {
            log.error("Field Cardinal Split failed. Field {0} value is not a list: {1}", fieldToSplit, fieldValue.getClass().getName());
            return;
          }
          List<?> fieldValues = (List<?>) fieldValue;
          if (fieldValues == null || fieldValues.isEmpty())
          {
            log.trace("Field to split value list is null.");
            appendFieldValuesAndSend(sourceGeoEvent, edOut, null, getSplitFieldOutputFieldCount(fdToSplit), -1);
          }
          if(fieldValues != null && !fieldValues.isEmpty()){
            int childId = 0;
            for (Object fv : fieldValues)
            {
              log.trace("Sending field value member {0}: {1}", childId, fv);
              appendFieldValuesAndSend(sourceGeoEvent, edOut, fv, 1, childId);
              childId++;
            }
          }
        }
      }
      catch (Exception e)
      {
        log.error("Field Cardinal Split failed. ", e);
      }
    }
  }

  private int sendFieldGroupMember(GeoEvent sourceGeoEvent, FieldDefinition fdToSplit, GeoEventDefinition edOut, int childId, FieldGroup fg) throws MessagingException
  {
    log.trace("Sending field group member {0}: {1}", childId, fg);
    appendFieldValuesAndSend(sourceGeoEvent, edOut, fg, getSplitFieldOutputFieldCount(fdToSplit), childId);
    childId++;
    return childId;
  }

  private int getSplitFieldOutputFieldCount(FieldDefinition fdToSplit)
  {
    List<FieldDefinition> childFieldDefinitions = fdToSplit.getChildren();
    return childFieldDefinitions == null || childFieldDefinitions.isEmpty() ? 1 : childFieldDefinitions.size();
  }

  private void appendFieldValuesAndSend(GeoEvent sourceGeoEvent, GeoEventDefinition edOut, Object v, int fieldCount, int childId) throws MessagingException
  {
    Object[] result = new Object[fieldCount + 1];
    if (v != null && v instanceof FieldGroup)
    {
      for (int index = 0; index < fieldCount; index++)
      {
        result[index] = ((FieldGroup) v).getField(index);
      }
    }
    else
    {
      result[0] = v;
    }
    result[fieldCount] = childId;

    int fieldToSplitIndex = sourceGeoEvent.getGeoEventDefinition().getIndexOf(fieldToSplit);
    Object[] allFieldValues = sourceGeoEvent.getAllFields();
    List<Object> valueList = new ArrayList<>();
    int index = 0;
    for (Object o : allFieldValues)
    {
      // if (o.getClass().toString().contains("Arrays$ArrayList"))
      // if (o instanceof ArrayList) not working!!!!!
      if (index == fieldToSplitIndex)
      {
        index++;
        continue;
      }
      valueList.add(o);
      index++;
    }

    createGeoEventAndSend(sourceGeoEvent, edOut, result, valueList, childId);
  }

  private void createGeoEventAndSend(GeoEvent sourceGeoEvent, GeoEventDefinition edOut, Object[] result, List<Object> valueList, int childId) throws MessagingException
  {
    GeoEvent geoEventOut = geoEventCreator.create(edOut.getGuid(), new Object[] { valueList.toArray(), result });
    geoEventOut.setProperty(GeoEventPropertyName.TYPE, "event");
    geoEventOut.setProperty(GeoEventPropertyName.OWNER_ID, getId());
    geoEventOut.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
    for (Map.Entry<GeoEventPropertyName, Object> property : sourceGeoEvent.getProperties())
    {
      if (!geoEventOut.hasProperty(property.getKey()))
      {
        geoEventOut.setProperty(property.getKey(), property.getValue());
      }
    }
    send(geoEventOut);
  }

  synchronized private GeoEventDefinition lookup(GeoEventDefinition edIn, FieldDefinition fdToSplit) throws Exception
  {
    GeoEventDefinition edOut = edMapper.containsKey(edIn.getGuid()) ? geoEventDefinitionManager.getGeoEventDefinition(edMapper.get(edIn.getGuid())) : null;
    if (edOut == null)
    {

      final List<FieldDefinition> fds = new ArrayList<>();
      Set<String> usedFieldNames = getOutputFieldNames(edIn, fdToSplit);
      List<FieldDefinition> childFieldDefinitions = fdToSplit.getChildren();
      if (childFieldDefinitions != null)
      {
        childFieldDefinitions.forEach(childFieldDef ->
        {
          try
          {
            fds.add((FieldDefinition) childFieldDef.clone());
          }
          catch (CloneNotSupportedException e)
          {
            log.info("Failed to clone group child field definition: {0}", childFieldDef != null ? childFieldDef.getName() : "NULL");
          }
        });
      }

      if (!fds.isEmpty())
      {
        log.trace("Augmenting definition to reduce split field and add splitfield childeren");
        updateChildFieldNames(usedFieldNames, fds);

      }
      else
      {
        log.trace("child field definitions of split field are null, using same definition.");
        FieldDefinition fd = (FieldDefinition) fdToSplit.clone();
        fd.setCardinality(FieldCardinality.One);
        fds.add(fd);
      }
      String newIndexFieldName = getUniqueFieldName(usedFieldNames, fieldToSplit + "_", INDEX_FIELD_NAME, true);
      FieldDefinition childFd = new DefaultFieldDefinition(newIndexFieldName, FieldType.Integer);
      // remove the split field, add the child fields, add the child index field
      edOut = edIn.reduce(Arrays.asList(fdToSplit.getName())).augment(fds).augment(Arrays.asList(childFd));
      edOut.setOwner(getId());
      if (!geoEventDefinitionName.isEmpty())
      {
        edOut.setName(geoEventDefinitionName);
        geoEventDefinitionManager.addTemporaryGeoEventDefinition(edOut, false);
      }
      else
      {
        geoEventDefinitionManager.addTemporaryGeoEventDefinition(edOut, true);
      }
      edMapper.put(edIn.getGuid(), edOut.getGuid());
    }
    return edOut;
  }

  private List<FieldDefinition> updateChildFieldNames(Set<String> usedFieldNames, List<FieldDefinition> childFieldDefs)
  {
    try
    {
      for (FieldDefinition childFieldDef : childFieldDefs)
  {
        childFieldDef.setName(getUniqueFieldName(usedFieldNames, fieldToSplit + "_", childFieldDef.getName(), false));
      }
    }
    catch (Exception e)
    {
      log.debug("Failed to set unique names on promoted child field group: {0}", e, childFieldDefs);
    }

    return childFieldDefs;
  }

  private Set<String> getOutputFieldNames(GeoEventDefinition edIn, FieldDefinition fdToSplit)
  {
    Set<String> usedFieldNames = new HashSet<>();
    List<FieldDefinition> fieldDefinitions = edIn.getFieldDefinitions();
    if (fieldDefinitions != null)
    {
      for (FieldDefinition field : fieldDefinitions)
      {
        if (field != null && field.getName() != null && !field.getName().equalsIgnoreCase(fdToSplit.getName()))
          addFieldName(usedFieldNames, field.getName());
      }
    }
    return usedFieldNames;
  }

  private String getUniqueFieldName(Set<String> usedFieldNames, String prefix, String baseName, boolean alwaysUsePrefix)
  {
    int newIndexFieldNameIndex = -1;
    String newIndexFieldName = baseName;
    if (prefix != null && alwaysUsePrefix)
      newIndexFieldName = prefix + baseName;

    log.trace("Making sure field name is unique: {0}", newIndexFieldName);
    try
    {
      boolean isExisting = containsFieldName(usedFieldNames, newIndexFieldName);
      while (isExisting && newIndexFieldNameIndex < 100)
      {
        log.trace("Found a matching name, incrementing index on {0}: {1}", newIndexFieldName, newIndexFieldNameIndex);
        ++newIndexFieldNameIndex;

        newIndexFieldName = prefix + baseName;
        if (newIndexFieldNameIndex >= 1)
          newIndexFieldName = prefix + baseName + "__" + newIndexFieldNameIndex;
        isExisting = containsFieldName(usedFieldNames, newIndexFieldName);
        if (!isExisting && newIndexFieldNameIndex >= 100)
          log.debug("Error adding new field, tried 100 times. giving up on {0}", newIndexFieldName);
      }
    }
    catch (Exception e)
    {
      log.debug("Failed to determine unque index field name", e);
    }
    addFieldName(usedFieldNames, newIndexFieldName);
    return newIndexFieldName;
  }

  private void addFieldName(Set<String> usedFieldNames, String name)
  {
    if (name != null)
      usedFieldNames.add(name.toLowerCase(Locale.ROOT));
  }

  private boolean containsFieldName(Set<String> usedFieldNames, String name)
  {
    return name != null && usedFieldNames.contains(name.toLowerCase(Locale.ROOT));
  }

  @Override
  public void disconnect()
  {
    if (geoEventProducer != null)
      geoEventProducer.disconnect();
  }

  @Override
  public String getStatusDetails()
  {
    return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
  }

  @Override
  public void init() throws MessagingException
  {
    // no-op, waiting for first event to create producer
  }

  @Override
  public boolean isConnected()
  {
    return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
  }

  @Override
  public void setup() throws MessagingException
  {
    // no-op, waiting for first event to create producer
  }

  @SuppressWarnings("deprecation") // Observable is required by the GeoEvent SDK; not under our control
  @Override
  public void update(Observable o, Object arg)
  {
    // no-op, waiting for first event to create producer
  }

  synchronized private void clearGeoEventDefinitionMapper()
  {
    if (!edMapper.isEmpty())
    {
      for (String guid : edMapper.values())
      {
        try
        {
          geoEventDefinitionManager.deleteGeoEventDefinition(guid);
        }
        catch (GeoEventDefinitionManagerException e)
        {
          // pass, just trying to clean up temporary definitions that may have been added
        }
      }
      edMapper.clear();
    }
  }

  @Override
  public Object addingService(ServiceReference<Object> reference)
  {
    Object service = definition.getBundleContext().getService(reference);
    if (service instanceof GeoEventDefinitionManager)
      this.geoEventDefinitionManager = (GeoEventDefinitionManager) service;
    return service;
  }

  @Override
  public void modifiedService(ServiceReference<Object> reference, Object service)
  {
    // no-op
  }

  @Override
  public void removedService(ServiceReference<Object> reference, Object service)
  {
    if (service instanceof GeoEventDefinitionManager)
    {
      clearGeoEventDefinitionMapper();
      this.geoEventDefinitionManager = null;
    }
  }

  @Override
  public void shutdown()
  {
    super.shutdown();
    clearGeoEventDefinitionMapper();
    final ExecutorService localExec = executor;
    executor = null;
    new Thread()
      {
        @Override
        public void run()
        {
          if (localExec != null)
          {
            try
            {
              localExec.shutdownNow();
              localExec.awaitTermination(10, TimeUnit.SECONDS);
            }
            catch (Exception e)
            {// pass, can't do anything now anyway
            }
          }
        }
      }.start();
  }

  class GeoEventSplitter implements Runnable
  {
    private final GeoEvent sourceGeoEvent;

    public GeoEventSplitter(GeoEvent sourceGeoEvent)
    {
      this.sourceGeoEvent = sourceGeoEvent;
    }

    @Override
    public void run()
    {
      try
      {
        fieldCardinalSplit(sourceGeoEvent);
      }
      catch (MessagingException e)
      {
        log.error("fieldCardinalSplit failed. ", e);
      }
    }
  }
}
