package com.esri.geoevent.processor.multicardinalfieldsplitter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class MulticardinalFieldSplitter extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable, ServiceTrackerCustomizer
{
  private static final Log                     log          = LogFactory.getLog(MulticardinalFieldSplitter.class);

  private Map<String, String>       edMapper  = new ConcurrentHashMap<String, String>();
  private ServiceTracker            geoEventDefinitionManagerTracker;
  private GeoEventDefinitionManager geoEventDefinitionManager;
  private Messaging                            messaging;
  private GeoEventCreator                      geoEventCreator;
  private GeoEventProducer                     geoEventProducer;
  private String                               fieldToSplit;
  private FieldDefinition                      fieldDefinitionToSplit;

  final Object                                 lock1        = new Object();

  private String geoEventDefinitionName;
  private ExecutorService                      executor;

  protected MulticardinalFieldSplitter(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
    if (geoEventDefinitionManagerTracker == null)
      geoEventDefinitionManagerTracker = new ServiceTracker(definition.getBundleContext(), GeoEventDefinitionManager.class.getName(), this);
    geoEventDefinitionManagerTracker.open();
    log.info("Event Splitter instantiated.");
  }

  public void afterPropertiesSet()
  {
    fieldToSplit = getProperty("fieldToSplit").getValueAsString();
    geoEventDefinitionName = getProperty("newGeoEventDefinitionName").getValueAsString().trim();
    if (geoEventDefinitionName.isEmpty())
      geoEventMutator = true;
    else
      geoEventMutator = false;
   
    executor = Executors.newFixedThreadPool(10);    
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
      GeoEventDefinition ed = sourceGeoEvent.getGeoEventDefinition();
      FieldDefinition fdToSplit = ed.getFieldDefinition(fieldToSplit);
      fieldDefinitionToSplit = fdToSplit;
      try
      {
        GeoEventDefinition edOut = lookup(sourceGeoEvent.getGeoEventDefinition());
        if (fieldDefinitionToSplit.getType() == FieldType.Group) 
        {
          List<FieldGroup> fieldgroups = sourceGeoEvent.getFieldGroups(fieldToSplit);
          if (fieldgroups == null || fieldgroups.size() == 0)
          {
            List<FieldDefinition> fds = fdToSplit.getChildren();
            appendFieldValuesAndSend(sourceGeoEvent, edOut, null, fds.size(), -1);
          }
          int childId = 0;
          for (FieldGroup fg : fieldgroups)
          {
            List<FieldDefinition> fds = fdToSplit.getChildren();
            appendFieldValuesAndSend(sourceGeoEvent, edOut, fg, fds.size(), childId);
            childId++;
          }                  
        }
        else
        {
          List<Object> fieldValues = (List<Object>)sourceGeoEvent.getField(fieldToSplit);
          if (fieldValues == null)
          {
            List<FieldDefinition> fds = fdToSplit.getChildren();
            appendFieldValuesAndSend(sourceGeoEvent, edOut, null, fds.size(), -1);
          }
          int childId = 0;
          for (Object fv : fieldValues)
          {
            appendFieldValuesAndSend(sourceGeoEvent, edOut, fv, 1, childId);            
            childId++;
          }
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
        log.error("fieldCardinalSplit failed. " + e.getMessage());
      }
    }
  }

  private void appendFieldValuesAndSend(GeoEvent sourceGeoEvent, GeoEventDefinition edOut, Object v, int fieldCount, int childId) throws MessagingException
  {
    Object[] result = new Object[fieldCount+1];
    if (v != null && v instanceof FieldGroup)
    {
      for(int index = 0; index < fieldCount; index++)
      {
          result[index] = ((FieldGroup)v).getField(index);
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
      //if (o.getClass().toString().contains("Arrays$ArrayList"))
      //if (o instanceof ArrayList) not working!!!!!
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

  
  @SuppressWarnings("unused")
  synchronized private GeoEventDefinition lookup(GeoEventDefinition edIn) throws Exception
  {
    GeoEventDefinition edOut = edMapper.containsKey(edIn.getGuid()) ? geoEventDefinitionManager.getGeoEventDefinition(edMapper.get(edIn.getGuid())) : null;
    if (edOut == null)
    {
      List<FieldDefinition> fds = fieldDefinitionToSplit.getChildren();
      FieldDefinition childFd = new DefaultFieldDefinition("childIndex", FieldType.Integer);
      
      if (fds != null)
      {
        edOut = edIn.augment(fds).reduce(Arrays.asList(fieldDefinitionToSplit.getName())).augment(Arrays.asList(childFd));        
      }
      else
      {
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
  public Object addingService(ServiceReference reference)
  {
    Object service = definition.getBundleContext().getService(reference);
    if (service instanceof GeoEventDefinitionManager)
      this.geoEventDefinitionManager = (GeoEventDefinitionManager) service;
    return service;
  }

  @Override
  public void modifiedService(ServiceReference reference, Object service)
  {
    ;
  }

  @Override
  public void removedService(ServiceReference reference, Object service)
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
    
    if (executor != null)
    {
      executor.shutdown();
      while (!executor.isTerminated()) {
      }
      executor = null;
    }
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
        log.error("fieldCardinalSplit failed. " + e.getMessage());
      }      
    }
  }  
}
