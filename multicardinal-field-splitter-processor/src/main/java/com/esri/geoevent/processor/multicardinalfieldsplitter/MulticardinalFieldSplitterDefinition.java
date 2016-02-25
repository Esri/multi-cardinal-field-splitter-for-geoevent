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
    return "10.4.0";
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
