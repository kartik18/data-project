import yaml
import os
print(os.getcwd())
# YML Parser
def get_tables():
    with open('project/configurations.yml', 'r') as f:
        data = yaml.safe_load(f)

    tables = {}
    for table_info in data.values():
        for tbl_dict in table_info:
            for table_name, details in tbl_dict.items():
                tables[table_name] = []
                for detail in details:
                    tables[table_name].append({
                        'file_column': detail.get('file_column'),
                        'table_mapped_column': detail.get('table_mapped_column'),
                        'type': detail.get('type'),
                        'validations': detail.get('validations')
                    })

    return tables

