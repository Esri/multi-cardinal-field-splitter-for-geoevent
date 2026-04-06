package com.esri.geoevent.processor.multicardinalfieldsplitter;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;

class MulticardinalFieldSplitterDefinitionTest
{
  private static MulticardinalFieldSplitterDefinition definition;

  @BeforeAll
  static void setUp()
  {
    definition = new MulticardinalFieldSplitterDefinition();
  }

  @Test
  void constructorCreatesFieldToSplitPropertyDefinition()
  {
    Map<String, PropertyDefinition> propDefs = definition.getPropertyDefinitions();
    assertTrue(propDefs.containsKey(MulticardinalFieldSplitterDefinition.PROPERTY_FIELD_TO_SPLIT),
        "Should contain 'fieldToSplit' property definition");

    PropertyDefinition pd = propDefs.get(MulticardinalFieldSplitterDefinition.PROPERTY_FIELD_TO_SPLIT);
    assertEquals("fieldToSplit", pd.getPropertyName());
    assertEquals(PropertyType.String, pd.getType());
    assertTrue(pd.isMandatory());
  }

  @Test
  void constructorCreatesNewGeoEventDefinitionNamePropertyDefinition()
  {
    Map<String, PropertyDefinition> propDefs = definition.getPropertyDefinitions();
    assertTrue(propDefs.containsKey(MulticardinalFieldSplitterDefinition.PROPERTY_NEW_GEOEVENT_DEFINITION_NAME),
        "Should contain 'newGeoEventDefinitionName' property definition");

    PropertyDefinition pd = propDefs.get(MulticardinalFieldSplitterDefinition.PROPERTY_NEW_GEOEVENT_DEFINITION_NAME);
    assertEquals("newGeoEventDefinitionName", pd.getPropertyName());
    assertEquals(PropertyType.String, pd.getType());
    assertTrue(pd.isMandatory());
  }

  @Test
  void fieldToSplitDefaultValue()
  {
    PropertyDefinition pd = definition.getPropertyDefinitions()
        .get(MulticardinalFieldSplitterDefinition.PROPERTY_FIELD_TO_SPLIT);
    assertNotNull(pd.getDefaultValue(), "fieldToSplit should have a default value");
    assertFalse(pd.getDefaultValue().toString().contains("${"),
        "Default value should not contain unresolved placeholders");
  }

  @Test
  void newGeoEventDefinitionNameDefaultValue()
  {
    PropertyDefinition pd = definition.getPropertyDefinitions()
        .get(MulticardinalFieldSplitterDefinition.PROPERTY_NEW_GEOEVENT_DEFINITION_NAME);
    assertNotNull(pd.getDefaultValue(), "newGeoEventDefinitionName should have a default value");
    assertFalse(pd.getDefaultValue().toString().contains("${"),
        "Default value should not contain unresolved placeholders");
  }

  @Test
  void hasTwoPropertyDefinitions()
  {
    assertEquals(2, definition.getPropertyDefinitions().size(),
        "Should have exactly 2 property definitions");
  }

  @Test
  void getVersionReturnsNonEmptyValue()
  {
    String version = definition.getVersion();
    assertNotNull(version);
    assertFalse(version.isEmpty(), "Version should not be empty");
    assertFalse(version.contains("${"), "Version should not contain unresolved placeholders");
  }

  @Test
  void getDomainReturnsNonEmptyValue()
  {
    String domain = definition.getDomain();
    assertNotNull(domain);
    assertFalse(domain.isEmpty(), "Domain should not be empty");
    assertFalse(domain.contains("${"), "Domain should not contain unresolved placeholders");
  }

  @Test
  void getNameReturnsNonEmptyValue()
  {
    String name = definition.getName();
    assertNotNull(name);
    assertFalse(name.isEmpty(), "Name should not be empty");
    assertFalse(name.contains("${"), "Name should not contain unresolved placeholders");
  }

  @Test
  void getLabelReturnsBundleValueFormat()
  {
    String label = definition.getLabel();
    assertNotNull(label);
    assertTrue(label.startsWith("${"), "Label should be a bundle value placeholder");
    assertTrue(label.endsWith("}"), "Label should be a bundle value placeholder");
    assertTrue(label.contains("PROCESSOR_LABEL"), "Label should reference PROCESSOR_LABEL key");
  }

  @Test
  void getDescriptionReturnsBundleValueFormat()
  {
    String description = definition.getDescription();
    assertNotNull(description);
    assertTrue(description.startsWith("${"), "Description should be a bundle value placeholder");
    assertTrue(description.contains("PROCESSOR_DESC"), "Description should reference PROCESSOR_DESC key");
  }

  @Test
  void getContactInfoReturnsBundleValueFormat()
  {
    String contactInfo = definition.getContactInfo();
    assertNotNull(contactInfo);
    assertTrue(contactInfo.startsWith("${"), "ContactInfo should be a bundle value placeholder");
    assertTrue(contactInfo.contains("PROCESSOR_CONTACT"), "ContactInfo should reference PROCESSOR_CONTACT key");
  }

  @Test
  void bundleValueContainsBundleSymbolicName()
  {
    String label = definition.getLabel();
    assertTrue(label.contains("com.esri.geoevent.processor.multicardinal-field-splitter-processor"),
        "Bundle value should contain the bundle symbolic name");
  }
}
