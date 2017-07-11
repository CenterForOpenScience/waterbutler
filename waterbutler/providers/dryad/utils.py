

def get_xml_element(xml, name):
    """Helper function for retrieving metadata fields from source document by name. Returns the text
    of the first tag named ``name`` or `None` if no such tag is found.

    :param xml.dom.minidom.Document xml: the xml document to fetch the element from
    :param str name: element tag name
    :rtype: str
    :return: string contents of element or None
    """
    el = xml.getElementsByTagName(name)
    if len(el) > 0:
        return el[0].firstChild.wholeText
    else:
        return None


def get_xml_element_list(xml, name):
    """Helper function for retrieving metadata fields from source document by name. Returns a list
    containing the text of all tags named ``name``.

    :param xml.dom.minidom.Document xml: the xml document to fetch the element from
    :param str name: element tag name
    :rtype: list
    :return: list of strings
    """
    return [i.firstChild.wholeText for i in xml.getElementsByTagName(name)]
