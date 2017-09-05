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

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class MulticardinalFieldSplitterDefinition extends GeoEventProcessorDefinitionBase
{
  final private static Log LOG = LogFactory.getLog(MulticardinalFieldSplitterDefinition.class);

  public MulticardinalFieldSplitterDefinition()
  {
    try
    {
      propertyDefinitions.put("fieldToSplit", new PropertyDefinition("fieldToSplit", PropertyType.String, "MyField", "Field to Split", "Field name its children to be split into individual GeoEvents", false, false));
      propertyDefinitions.put("newGeoEventDefinitionName", new PropertyDefinition("newGeoEventDefinitionName", PropertyType.String, "FieldgroupSplitter", "Resulting GeoEvent Definition Name", "FieldSplitter", false, false));
    }
    catch (Exception e)
    {
      LOG.error("Error setting up Multicardinal Field Splitter Definition.", e);
    }
  }

  @Override
  public String getVersion()
  {
    return "10.5.0";
  }

  @Override
  public String getDomain()
  {
    return "com.esri.geoevent.processor";
  }

  @Override
  public String getName()
  {
    return "MulticardinalFieldSplitter";
  }

  @Override
  public String getLabel()
  {
    return "Multicardinal Field Splitter";
  }

  @Override
  public String getDescription()
  {
    return "Split multi cardinality field into individual GeoEvents";
  }

  @Override
  public String getContactInfo()
  {
    return "mpilouk@esri.com";
  }

}
