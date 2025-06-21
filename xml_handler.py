import xml.etree.ElementTree as ET
import uuid

# NEMSIS specific namespaces (if any, often they are not explicitly namespaced in files)
# If namespaces are used, they look like: NS_MAP = {'nem': 'http://www.nemsis.org'}
# And find calls would be like: root.find('nem:Header', NS_MAP)
# For the provided example, it seems like direct tag names are used without explicit namespace prefixes in find calls.


def _sanitize_name(name):
    """Converts a name to be SQL-friendly. Replaces . with _ and removes other non-alphanumeric chars."""
    # Replace . with _ first, then remove other problematic characters
    name = name.replace(".", "_")
    # Keep only alphanumeric and underscores
    name = "".join(char for char in name if char.isalnum() or char == "_")
    # Ensure it doesn't start with a number (common SQL restriction)
    if name and name[0].isdigit():
        name = "_" + name
    return name if name else "unnamed_element"


def _traverse_element_recursive(
    element, parent_element_id, current_pcr_uuid, element_path_parts, processed_elements, parent_table_suggestion_for_child
):
    """Recursively traverses XML elements and collects their data."""
    element_id = str(uuid.uuid4())  # Unique ID for this element instance

    raw_tag = element.tag
    local_tag_name = raw_tag
    # Strip namespace URI if present (e.g., {http://www.nemsis.org}TagName -> TagName)
    if "}" in raw_tag and raw_tag.startswith("{"):
        local_tag_name = raw_tag.split("}", 1)[1]

    sanitized_local_tag = _sanitize_name(local_tag_name)

    # table_suggestion will be based on the sanitized local tag name
    table_suggestion = sanitized_local_tag
    current_path_parts = element_path_parts + [
        sanitized_local_tag
    ]  # Path uses sanitized local names
    element_path_str = "/".join(current_path_parts)

    # Sanitize attribute keys as well, stripping namespaces if they are present in {uri}key format
    attributes = {}
    for k_raw, v in element.attrib.items():
        k_local = k_raw
        if "}" in k_raw and k_raw.startswith("{"):
            k_local = k_raw.split("}", 1)[1]
        attributes[_sanitize_name(k_local)] = v

    # Capture the PatientCareReport UUID if this element is it or is a child of it
    # Use local_tag_name for comparison here, assuming NEMSIS tags won't be namespaced differently
    if local_tag_name == "PatientCareReport" and element.get("UUID"):
        current_pcr_uuid = element.get("UUID")
        # Add the PCR UUID as an attribute of the PatientCareReport element itself if not already there
        # and if the sanitized key "UUID" doesn't exist (it might if UUID was an explicit attribute)
        if _sanitize_name("UUID") not in attributes:
            attributes[_sanitize_name("UUID")] = current_pcr_uuid

    element_data = {
        "element_id": element_id,
        "parent_element_id": parent_element_id,
        "element_tag": local_tag_name,  # Store the local tag name (without namespace)
        "full_xmlns_tag": raw_tag,  # Store the original tag with xmlns if needed later for any reason
        "table_suggestion": table_suggestion,  # Based on sanitized local tag
        "attributes": attributes,  # Attributes are also sanitized (keys)
        "text_content": element.text.strip() if element.text else None,
        "pcr_uuid_context": current_pcr_uuid,
        "parent_table_suggestion": parent_table_suggestion_for_child,
    }
    processed_elements.append(element_data)

    for child in element:
        _traverse_element_recursive(
            child, element_id, current_pcr_uuid, current_path_parts, processed_elements, element_data["table_suggestion"]
        )


def parse_xml_file(file_path):
    """Parses XML and returns all elements with their contextual data.

    Args:
        file_path: Path to the XML file.

    Returns:
        A list of dictionaries, where each dictionary represents an XML element.
        Returns an empty list if parsing fails or file not found.
    """
    processed_elements = []
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file {file_path}: {e}")
        return []
    except FileNotFoundError:
        print(f"Error: XML file not found at {file_path}")
        return []

    _traverse_element_recursive(root, None, None, [], processed_elements, None)

    return processed_elements

    print("Testing xml_handler.py with full traversal...")
    example_file = "nemsis_xml/NEMSIS_EMS__3_5_0__20250215_121500.xml"

    elements_data = parse_xml_file(example_file)

    if elements_data:
        print(f"Successfully parsed {len(elements_data)} elements from {example_file}.")
        # Check a few elements for pcr_uuid_context
        pcr_contexts_found = set()
        for el_data in elements_data:
            if el_data.get("pcr_uuid_context"):
                pcr_contexts_found.add(el_data["pcr_uuid_context"])

        print(
            f"\nUnique PCR UUID Contexts found in elements: {pcr_contexts_found if pcr_contexts_found else 'None'}"
        )

        for i, el_data in enumerate(elements_data[:2]):  # Print first 2 elements
            print(f"\nElement {i+1}:")
            print(f"  ID: {el_data['element_id']}")
            print(f"  Tag: {el_data['element_tag']}")
            print(
                f"  PCR UUID Context (element-specific): {el_data['pcr_uuid_context']}"
            )
        if len(elements_data) > 2:
            print(f"\n... and {len(elements_data) - 2} more elements.")
    else:
        print(f"No data extracted or error during parsing from {example_file}.")

    # Test with a non-existent file
    print("\nTesting with a non-existent file:")
    elements_data_nf = parse_xml_file("non_existent_file.xml")
    if not elements_data_nf:
        print("Correctly handled non-existent file, returned empty list.")
