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
      propertyDefinitions.put("fieldToSplit", new PropertyDefinition("fieldToSplit", PropertyType.String, "MyField", "Field to Split", "The name of a group field to have its children to be split into individual GeoEvents. One event will be created for each child of this group field. The group field will be removed from the resulting GeoEvent Definition, and child fields will be promoted.", true, false));
      propertyDefinitions.put("newGeoEventDefinitionName", new PropertyDefinition("newGeoEventDefinitionName", PropertyType.String, "FieldgroupSplitter", "New GeoEvent Definition Name", "The name of the new GeoEvent Definition that will be created.", true, false));
    }
    catch (Exception e)
    {
      LOG.error("Error setting up Multicardinal Field Splitter Definition.", e);
    }
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
    return "Release 8: Split multi-cardinal group fields into individual GeoEvents. For fields of type 'Group' that have cardinality set a 'Many', this processor will create a copy of the incoming event for each child of the specified group field. The attributes of each child will be promoted up one level. All other attributes of the original event will be preserved. Any promoted field whose name collides with an existing name, will be modified to include the parent field name as a prefix and possibly '__#' at the end (where # will be a unique number).";
  }

  @Override
  public String getContactInfo()
  {
    return "geoevent@esri.com";
  }

}
