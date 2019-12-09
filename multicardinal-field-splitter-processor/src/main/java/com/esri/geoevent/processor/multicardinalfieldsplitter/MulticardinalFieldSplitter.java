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
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  private Map<String, String>       edMapper         = new ConcurrentHashMap<String, String>();
  private ServiceTracker<?, ?>      geoEventDefinitionManagerTracker;
  private GeoEventDefinitionManager geoEventDefinitionManager;
  private Messaging                 messaging;
  private GeoEventCreator           geoEventCreator;
  private GeoEventProducer          geoEventProducer;
  private String                    fieldToSplit;
  private FieldDefinition           fieldDefinitionToSplit;

  final Object                      lock1            = new Object();

  private String                    geoEventDefinitionName;
  private ExecutorService           executor;

  protected MulticardinalFieldSplitter(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
    if (geoEventDefinitionManagerTracker == null)
      geoEventDefinitionManagerTracker = new ServiceTracker<Object, Object>(definition.getBundleContext(), GeoEventDefinitionManager.class.getName(), this);
    geoEventDefinitionManagerTracker.open();
    log.trace("Event Splitter instantiated.");
  }

  public void afterPropertiesSet()
  {
    fieldToSplit = getProperty("fieldToSplit").getValueAsString();
    geoEventDefinitionName = getProperty("newGeoEventDefinitionName").getValueAsString().trim();
    if (geoEventDefinitionName.isEmpty())
      geoEventMutator = true;
    else
      geoEventMutator = false;
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
    GeoEventSplitter splitter = new GeoEventSplitter(geoEvent);
    if (executor == null || executor.isShutdown() || executor.isTerminated())
      executor = Executors.newCachedThreadPool();
    executor.execute(splitter);

    return null;
  }

  @Override
  public List<EventDestination> getEventDestinations()
  {
    return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<EventDestination>();
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    List<String> errors = new ArrayList<String>();
    if (errors.size() > 0)
    {
      StringBuffer sb = new StringBuffer();
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

  @SuppressWarnings("unchecked")
  private void fieldCardinalSplit(GeoEvent sourceGeoEvent) throws MessagingException
  {
    if (geoEventCreator != null)
    {
      try
      {
        GeoEventDefinition ed = sourceGeoEvent.getGeoEventDefinition();
        FieldDefinition fdToSplit = ed.getFieldDefinition(fieldToSplit);
        fieldDefinitionToSplit = fdToSplit;

        GeoEventDefinition edOut = lookup(sourceGeoEvent.getGeoEventDefinition());
        if (Thread.interrupted())
          return;
        if (fieldDefinitionToSplit.getType() == FieldType.Group)
        {
          log.trace("Field definition to split is a group");
          List<FieldGroup> fieldgroups = sourceGeoEvent.getFieldGroups(fieldToSplit);
          if (fieldgroups == null || fieldgroups.size() == 0)
          {
            log.trace("Child field groups is null or size 0.");
            List<FieldDefinition> fds = fdToSplit.getChildren();
            appendFieldValuesAndSend(sourceGeoEvent, edOut, null, fds.size(), -1);
          }
          int childId = 0;
          for (FieldGroup fg : fieldgroups)
          {
            log.trace("Sending field group member {0}: {1}", childId, fg);
            List<FieldDefinition> fds = fdToSplit.getChildren();
            appendFieldValuesAndSend(sourceGeoEvent, edOut, fg, fds.size(), childId);
            childId++;
          }
        }
        else
        {
          log.debug("Field defintion to split is not a group: {0}", fieldDefinitionToSplit.getType());
          List<Object> fieldValues = (List<Object>) sourceGeoEvent.getField(fieldToSplit);
          if (fieldValues == null || fieldValues.size() <= 0)
          {
            log.trace("Field to split value list is null.");
            List<FieldDefinition> fds = fdToSplit.getChildren();
            appendFieldValuesAndSend(sourceGeoEvent, edOut, null, fds.size(), -1);
          }
          int childId = 0;
          for (Object fv : fieldValues)
          {
            log.trace("Sending field value member {0}: {1}", childId, fv);
            appendFieldValuesAndSend(sourceGeoEvent, edOut, fv, 1, childId);
            childId++;
          }
        }
      }
      catch (Exception e)
      {
        log.error("Field Cardinal Split failed. ", e);
      }
    }
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
      result[fieldCount] = childId;
    }
    else
    {
      result[0] = v;
      result[1] = childId;
    }

    int fieldToSplitIndex = sourceGeoEvent.getGeoEventDefinition().getIndexOf(fieldToSplit);
    Object[] allFieldValues = sourceGeoEvent.getAllFields();
    List<Object> valueList = new ArrayList<Object>();
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

  synchronized private GeoEventDefinition lookup(GeoEventDefinition edIn) throws Exception
  {
    GeoEventDefinition edOut = edMapper.containsKey(edIn.getGuid()) ? geoEventDefinitionManager.getGeoEventDefinition(edMapper.get(edIn.getGuid())) : null;
    if (edOut == null)
    {
      List<FieldDefinition> fds = fieldDefinitionToSplit.getChildren();

      String newIndexFieldName = getUniqueFieldName(edIn, INDEX_FIELD_NAME);
      FieldDefinition childFd = new DefaultFieldDefinition(newIndexFieldName, FieldType.Integer);

      if (fds != null)
      {
        log.trace("Augmenting definition to reduce split field and add splitfield childeren");
        edOut = edIn.reduce(Arrays.asList(fieldDefinitionToSplit.getName())).augment(fds).augment(Arrays.asList(childFd));
      }
      else
      {
        log.trace("child field definitions of split field are null, using same definition.");
        FieldDefinition fd = (FieldDefinition) fieldDefinitionToSplit.clone();
        fd.setCardinality(FieldCardinality.One);
        edOut = edIn.reduce(Arrays.asList(fieldDefinitionToSplit.getName())).augment(Arrays.asList(fd)).augment(Arrays.asList(childFd));
      }
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

  private String getUniqueFieldName(GeoEventDefinition edIn, String baseName)
  {
    int newIndexFieldNameIndex = 0;
    String newIndexFieldName = baseName + "__" + newIndexFieldNameIndex;

    try
    {
      FieldDefinition isExisting = edIn.getFieldDefinition(newIndexFieldName);
      while (isExisting != null && newIndexFieldNameIndex < 100)
      {
        ++newIndexFieldNameIndex;
        newIndexFieldName = baseName + "__" + newIndexFieldNameIndex;
        isExisting = edIn.getFieldDefinition(newIndexFieldName);
      }
    }
    catch (Exception e)
    {
      log.debug("Failed to determine unque index field name", e);
    }
    return newIndexFieldName;
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
    ;
  }

  @Override
  public boolean isConnected()
  {
    return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
  }

  @Override
  public void setup() throws MessagingException
  {
    ;
  }

  @Override
  public void update(Observable o, Object arg)
  {
    ;
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
          ;
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
    ;
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
    private GeoEvent sourceGeoEvent;

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
