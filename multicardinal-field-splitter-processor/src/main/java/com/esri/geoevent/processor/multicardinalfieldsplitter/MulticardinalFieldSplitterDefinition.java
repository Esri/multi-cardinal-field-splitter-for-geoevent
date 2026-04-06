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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class MulticardinalFieldSplitterDefinition extends GeoEventProcessorDefinitionBase
{
  private static final BundleLogger LOG = BundleLoggerFactory.getLogger(MulticardinalFieldSplitter.class);
  private static final String RESOURCE_PATH = "com/esri/geoevent/processor/multicardinal-field-splitter-processor.properties";
  private static final String BUNDLE_SYMBOLIC_NAME = "com.esri.geoevent.processor.multicardinal-field-splitter-processor";
  
  static final String PROPERTY_FIELD_TO_SPLIT = "fieldToSplit";
  static final String PROPERTY_NEW_GEOEVENT_DEFINITION_NAME = "newGeoEventDefinitionName";

  private static final String KEY_PROCESSOR_VERSION = "PROCESSOR_VERSION";
  private static final String KEY_PROCESSOR_NAME = "PROCESSOR_NAME";
  private static final String KEY_PROCESSOR_DOMAIN = "PROCESSOR_DOMAIN";
  private static final String KEY_FIELD_TO_SPLIT_DEFAULT = "FIELD_TO_SPLIT_DEFAULT";
  private static final String KEY_NEW_GEOEVENT_DEFINITION_NAME_DEFAULT = "NEW_GEOEVENT_DEFINITION_NAME_DEFAULT";
  private static final String FALLBACK_PROCESSOR_VERSION = "10.6.0";
  private static final String FALLBACK_PROCESSOR_NAME = "MulticardinalFieldSplitter";
  private static final String FALLBACK_PROCESSOR_DOMAIN = "com.esri.geoevent.processor";
  private static final String FALLBACK_FIELD_TO_SPLIT_DEFAULT = "MyField";
  private static final String FALLBACK_NEW_GEOEVENT_DEFINITION_NAME_DEFAULT = "FieldgroupSplitter";

  private static final Properties DEFAULTS = loadDefaults();
  private static final String PROCESSOR_VERSION = getConfiguredValue(KEY_PROCESSOR_VERSION, FALLBACK_PROCESSOR_VERSION);
  private static final String PROCESSOR_NAME = getConfiguredValue(KEY_PROCESSOR_NAME, FALLBACK_PROCESSOR_NAME);
  private static final String PROCESSOR_DOMAIN = getConfiguredValue(KEY_PROCESSOR_DOMAIN, FALLBACK_PROCESSOR_DOMAIN);
  private static final String FIELD_TO_SPLIT_DEFAULT = getConfiguredValue(KEY_FIELD_TO_SPLIT_DEFAULT, FALLBACK_FIELD_TO_SPLIT_DEFAULT);
  private static final String NEW_GEOEVENT_DEFINITION_NAME_DEFAULT = getConfiguredValue(KEY_NEW_GEOEVENT_DEFINITION_NAME_DEFAULT, FALLBACK_NEW_GEOEVENT_DEFINITION_NAME_DEFAULT);

  private static final String PROCESSOR_LABEL = bundleValue("PROCESSOR_LABEL");
  private static final String PROCESSOR_DESC = bundleValue("PROCESSOR_DESC");
  private static final String PROCESSOR_CONTACT = bundleValue("PROCESSOR_CONTACT");
  private static final String FIELD_TO_SPLIT_LABEL = bundleValue("FIELD_TO_SPLIT_LABEL");
  private static final String FIELD_TO_SPLIT_DESC = bundleValue("FIELD_TO_SPLIT_DESC");
  private static final String NEW_GEOEVENT_DEFINITION_NAME_LABEL = bundleValue("NEW_GEOEVENT_DEFINITION_NAME_LABEL");
  private static final String NEW_GEOEVENT_DEFINITION_NAME_DESC = bundleValue("NEW_GEOEVENT_DEFINITION_NAME_DESC");

  public MulticardinalFieldSplitterDefinition()
  {
    try
    {
      propertyDefinitions.put(PROPERTY_FIELD_TO_SPLIT, new PropertyDefinition(PROPERTY_FIELD_TO_SPLIT, PropertyType.String, FIELD_TO_SPLIT_DEFAULT, FIELD_TO_SPLIT_LABEL, FIELD_TO_SPLIT_DESC, true, false));
      propertyDefinitions.put(PROPERTY_NEW_GEOEVENT_DEFINITION_NAME, new PropertyDefinition(PROPERTY_NEW_GEOEVENT_DEFINITION_NAME, PropertyType.String, NEW_GEOEVENT_DEFINITION_NAME_DEFAULT, NEW_GEOEVENT_DEFINITION_NAME_LABEL, NEW_GEOEVENT_DEFINITION_NAME_DESC, true, false));
    }
    catch (Exception e)
    {
      LOG.error("Error setting up Multicardinal Field Splitter Definition.", e);
    }
  }

  @Override
  public String getVersion()
  {
    return PROCESSOR_VERSION;
  }

  @Override
  public String getDomain()
  {
    // return "com.esri.geoevent.processor";
    return PROCESSOR_DOMAIN;
  }

  @Override
  public String getName()
  {
    // return "MulticardinalFieldSplitter";
    return PROCESSOR_NAME;
  }

  @Override
  public String getLabel()
  {
    // return "Multicardinal Field Splitter";
    return PROCESSOR_LABEL;
  }

  @Override
  public String getDescription()
  {
    // return "Release 9: Split multi-cardinal group fields into individual GeoEvents. For fields of type 'Group' that have cardinality set a 'Many', this processor will create a copy of the incoming event for each child of the specified group field. The attributes of each child will be promoted up one level. All other attributes of the original event will be preserved. Any promoted field whose name collides with an existing name, will be modified to include the parent field name as a prefix and possibly '__#' at the end (where # will be a unique number).";
    return PROCESSOR_DESC;
  }

  @Override
  public String getContactInfo()
  {
    // return "geoevent@esri.com";
    return PROCESSOR_CONTACT;
  }

    private static Properties loadDefaults()
  {
    Properties properties = new Properties();
    try (InputStream inputStream = MulticardinalFieldSplitterDefinition.class.getClassLoader().getResourceAsStream(RESOURCE_PATH))
    {
      if (inputStream == null)
      {
        LOG.warn("Defaults resource \"{0}\" was not found. Built-in defaults will be used.", RESOURCE_PATH);
        return properties;
      }

      properties.load(inputStream);
    }
    catch (IOException e)
    {
      LOG.warn("Unable to load defaults resource \"{0}\". Built-in defaults will be used.", e, RESOURCE_PATH);
    }
    return properties;
  }

  private static String getConfiguredValue(String key, String fallback)
  {
    String value = DEFAULTS.getProperty(key);
    if (value == null)
      return fallback;

    String normalizedValue = value.trim();
    if (normalizedValue.isEmpty() || normalizedValue.contains("${"))
      return fallback;

    return normalizedValue;
  }

  private static String bundleValue(String key)
  {
    return "${" + BUNDLE_SYMBOLIC_NAME + "." + key + "}";
  }

}
