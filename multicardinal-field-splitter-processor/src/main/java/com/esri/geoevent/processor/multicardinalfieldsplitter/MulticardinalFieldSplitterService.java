/*
  Copyright 2017 Esri

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