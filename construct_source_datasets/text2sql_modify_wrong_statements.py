import json
import os
import sys


def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:  # Unix (e.g. Linux, macOS)
        os.system('clear')


def get_verification():
    while True:
        user_input = input("Input (T or F): ").strip()
        if user_input in ['T', 'F', 'PASS']:
            return user_input
        else:
            print(f"'{user_input}' is wrong input. Please input 'T' or 'F'.")


def get_modified_statement():
    user_input = input("Modified statement: ").strip()
    return user_input


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.display import table_visualization
    return table_visualization


if __name__ == '__main__':
    table_visualization = import_utils()

    with open('storage/data_set_with_generated_statement.json', 'r') as file:
            data_set_with_generated_statement = json.load(file)

    coverage_scores = dict()
    for idx, instance in enumerate(data_set_with_generated_statement):
        for jdx, data in enumerate(instance['data_list']):
            # Calculate SQL query result coverage for each data
            flattened_cell = [cell for row in data['sql_query_result']['cell'] for cell in row]
            score = sum(str(cell).strip().lower() in data['statement'].lower() for cell in flattened_cell) / len(flattened_cell) * 100
            coverage_scores[(idx, jdx)] = score
    
    # coverage score < 100
    data_subset_with_wrong_statement = [
        {
            'key': (idx, jdx),
            'score': score,
            'gold_table_id_set': data_set_with_generated_statement[idx]['gold_table_id_set'],
            'data': data_set_with_generated_statement[idx]['data_list'][jdx]
        }
        for (idx, jdx), score in coverage_scores.items()
        if score != 100.0
    ]
    
    # (key, score, nl_query, statement, sql_query_result)
    info_list = [
        (
            data['key'],
            data['score'],
            data['data']['nl_query'],
            data['data']['statement'],
            data['data']['sql_query_result']
        )
        for data in data_subset_with_wrong_statement
    ]

    info_list_sorted_by_score = sorted(info_list, key=lambda x: x[1])

    modified_statement_set_with_human_annotation = dict()
    pass_flag = False

    for i, info in enumerate(info_list_sorted_by_score):
        key, score, nl_query, statement, sql_query_result = info

        if pass_flag:
            modified_statement_set_with_human_annotation[f"{key[0]}-{key[1]}"] = "NEED TO MODIFY"
            continue

        clear_screen()

        print(
            f"{i + 1} / {len(info_list_sorted_by_score)}",
            f"Key: {key}",
            f"Score: {score}",
            f"NL query: {nl_query}",
            f"Statement: {statement}",
            f"SQL query result:{table_visualization(table_num=-1, metadata=None, header=sql_query_result['header'], cell=sql_query_result['cell'])}",
            sep="\n"
        )

        verification = get_verification()
        if verification == 'F':
            modified_statement_set_with_human_annotation[f"{key[0]}-{key[1]}"] = get_modified_statement()
        
        elif verification == 'PASS':
            modified_statement_set_with_human_annotation[f"{key[0]}-{key[1]}"] = "NEED TO MODIFY"
            pass_flag = True
            
    with open('storage/modified_statement_set_with_human_annotation.json', 'w') as file:
        json.dump(modified_statement_set_with_human_annotation, file, indent=4)
