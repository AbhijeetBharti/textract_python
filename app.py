import base64
import boto3, json, os
from collections import defaultdict


def extract_table_form(textract_data):
    # Initialize dictionaries for form and table contents
    form_data = {}
    table_data = []

    # Parse blocks for forms and tables
    for block in textract_data['Blocks']:
        if block['BlockType'] == 'KEY_VALUE_SET' and 'KEY' in [entity for entity in block.get('EntityTypes', [])]:
            # Extract key-value pairs for forms
            key = None
            value = None
            for relationship in block.get('Relationships', []):
                if relationship['Type'] == 'CHILD':
                    key_texts = []
                    for child_id in relationship['Ids']:
                        key_block = next((b for b in textract_data['Blocks'] if b['Id'] == child_id), None)
                        if key_block and key_block['BlockType'] == 'WORD' and key_block.get('Text'):
                            key_texts.append(key_block.get('Text'))

                    key = ' '.join(key_texts)

                elif relationship['Type'] == 'VALUE':
                    key_values = []
                    for child_id in relationship['Ids']:
                        value_block = next((b for b in textract_data['Blocks'] if b['Id'] == child_id), None)
                        if value_block:
                            for relationship1 in value_block.get('Relationships', []):
                                if relationship1['Type'] == 'CHILD':
                                    for child_id1 in relationship1['Ids']:
                                        value_block1 = next((b for b in textract_data['Blocks'] if b['Id'] == child_id1), None)
                                        if value_block1 and value_block1['BlockType'] == 'WORD' and value_block1.get('Text'):
                                            key_values.append(value_block1.get('Text'))

                    value = ' '.join(key_values)
                            
            if key and value:
                form_data[key] = value

        elif block['BlockType'] == 'TABLE':
            # Extract table content
            rows = defaultdict(list)
            for relationship in block.get('Relationships', []):
                if relationship['Type'] == 'CHILD':
                    for child_id in relationship['Ids']:
                        cell = next((b for b in textract_data['Blocks'] if b['Id'] == child_id), None)
                        if cell and cell['BlockType'] == 'CELL':
                            row_index = cell['RowIndex']
                            text = []
                            for cell_relation in cell.get('Relationships', []):
                                if cell_relation['Type'] == 'CHILD':
                                    for child_id1 in cell_relation['Ids']:
                                        child = next((b for b in textract_data['Blocks'] if b['Id'] == child_id1), None)
                                        if child and child['BlockType'] == 'WORD' and child.get('Text'):
                                            text.append(child.get('Text'))

                            rows[row_index].append(' '.join(text))

            for row_index in sorted(rows.keys()):
                table_data.append(rows[row_index])

    result = {
        "form_data": form_data,
        "table_data": table_data,
    }

    return result


def lambda_handler(event, context):
    print("Abhijeet")
    print(event)
    
    if 'image' not in event:
        return {'error': 'No file provided'}, 400
    
    base64_image = event['image']
    image_bytes = base64.b64decode(base64_image)

    textract = boto3.client('textract')
    response = textract.analyze_document(
        Document={'Bytes': image_bytes}
        , FeatureTypes=["TABLES", "FORMS"]
    )

    response = extract_table_form(response)
    return response
