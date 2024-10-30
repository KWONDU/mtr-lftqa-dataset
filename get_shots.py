import json


def get_expand_statement_with_low_header_sim_task_shots():
    return "\n".join([
        "\n".join([
            f"**Example {idx * 2 + ddx + 1}:**",
            "- **DataFrame schema set**:",
            "\n".join([
                "\n".join([
                    f"\t- **DataFrame {tdx + 1}**:",
                    f"\t- **Caption**: {table['metadata']}",
                    f"\t- **Columns**: {' | '.join(table['header'])}",
                    f"\t- **First row**: {' | '.join(table['cell'][0])}",
                    ""
                ])
                for tdx, table in enumerate(shot['gold_table_set'])
            ]),
            "- **SQL query**:",
            shot['data_list'][ddx]['sql_query'],
            "",
            "- **Statement**:",
            shot['data_list'][ddx]['short_statement'],
            "",
            "- **Python code**:",
            shot['data_list'][ddx]['python_code'],
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_LOW_HEADER_SIM)
        for ddx in range(2)
    ])


def get_expand_statement_with_high_header_sim_task_shots():
    return "\n".join([
        "\n".join([
            f"**Example {idx + 1}:**",
            "- **DataFrame schema**:",
            f"\t- **Caption**: {shot['gold_table_set'][0]['metadata']}",
            f"\t- **Columns**: {' | '.join(shot['gold_table_set'][0]['header'])}",
            f"\t- **First row**: {' | '.join(shot['gold_table_set'][0]['cell'][0])}",
            "",
            "- **SQL query**:",
            shot['data_list'][0]['sql_query'],
            "",
            "- **Statement**:",
            shot['data_list'][0]['short_statement'],
            "",
            "- **Python code**:",
            shot['data_list'][0]['python_code'],
            ""
        ])
        for idx, shot in enumerate(SHOTS_WITH_HIGH_HEADER_SIM)
    ])


if __name__ == 'get_shots':
    with open('shots/shots_with_high_header_sim.json', 'r') as file:
        SHOTS_WITH_HIGH_HEADER_SIM = json.load(file)
    MOD_FOUR = 0
    
    with open('shots/shots_with_low_header_sim.json', 'r') as file:
        SHOTS_WITH_LOW_HEADER_SIM = json.load(file)
