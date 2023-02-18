
import xml.etree.ElementTree as ET
import json
import csv
import operator


def read_csv_file(filename):
    # Dictionary Object to store data:
    data = []
    # Open CSV File:
    with open(filename, 'r') as file:
        # Pull data from CSV:
        reader = csv.DictReader(file)
        # Loop through file records,
        for row in reader:
            data.append(row)
    return data


def write_csv_file(filename, data):
    """
    Takes a filename (to be writen to) and a data object 
    (created by one of the read_*_file functions). 
    Writes the data in the CSV format.
    """
    # Generate instance of new file to write:
    with open(filename, "w", newline="") as outfile:
        # Pass CSV file and dictionary keys as column headers:
        writer = csv.DictWriter(outfile, fieldnames=data[0].keys())

        writer.writeheader()

        for entry in data:
            writer.writerow(entry)


def read_json_file(filename):
    """
    Similar to read_csv_file, except works for JSON files.
    """
    with open(filename) as json_file:
        data = json.load(json_file)
    return data


def write_json_file(filename, data):
    """
    Writes JSON files. Similar to write_csv_file.
    """
    with open(filename, 'w') as file:
        json_new = json.dumps(data)
        file.write(json_new)


def read_xml_file(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data_list = []
    for record in root:
        record_dict = {}
        for column in record:
            record_dict.update({column.tag: column.text})
        data_list.append(record_dict)
    return data_list


def write_xml_file(filename,data_list):
    # there should be a single "data" node,
    root = ET.Element("data")
    for record in data_list:
        # with as many record nodes as needed
        record_node = ET.SubElement(root, "record")
        for column, value in record.items():
            # in each record is column node with text content for that record
            column_node = ET.SubElement(record_node, column)
            column_node.text = value
    tree = ET.ElementTree(root)
    tree.write(filename)

