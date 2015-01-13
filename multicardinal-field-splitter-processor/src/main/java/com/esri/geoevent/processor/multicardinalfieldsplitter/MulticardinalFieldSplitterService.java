package com.esri.geoevent.processor.multicardinalfieldsplitter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class MulticardinalFieldSplitterService extends GeoEventProcessorServiceBase
{
  private Messaging messaging;
  final private static Log LOG = LogFactory.getLog(MulticardinalFieldSplitterService.class);

  public MulticardinalFieldSplitterService()
  {
    definition = new MulticardinalFieldSplitterDefinition();
    LOG.info("EventSplitterService instantiated.");
  }

  @Override
  public GeoEventProcessor create() throws ComponentException
  {
    MulticardinalFieldSplitter detector = new MulticardinalFieldSplitter(definition);
    detector.setMessaging(messaging);
    return detector;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }
}